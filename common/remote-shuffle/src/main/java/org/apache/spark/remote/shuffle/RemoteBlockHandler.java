/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remote.shuffle;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.*;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.remote.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.spark.network.shuffle.RemoteBlockPushResolver.ATTEMPT_ID_KEY;
import static org.apache.spark.network.shuffle.RemoteBlockPushResolver.MERGE_DIR_KEY;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

// remote worker
public class RemoteBlockHandler extends ExternalBlockHandler {
    private static final Logger logger = LoggerFactory.getLogger(RemoteBlockHandler.class);
    private final String masterHost;
    private final int masterPort;
    private final String localHost;
    private final int localPort;
    private final TransportConf transportConf;
    private TransportClientFactory clientFactory;
    private TransportClient client;
    private final int MAX_ATTEMPTS = 3;

    private final int subDirsPerLocalDir;
    private final long heartbeatInterval;
    private final long monitorInterval;
    private final Executor mergedDirCleaner;

    private List<ShuffleDir> workDirs = new ArrayList<>();
    // key: appid_attempt  value: v-key: shuffle_key v-value: runningStage
    private ConcurrentHashMap<String, ConcurrentHashMap<String, RunningStage>> appStageMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> localMergeDirs = new ConcurrentHashMap<>();

    private final ScheduledExecutorService heartbeatThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("remote-shuffle-worker-heartbeat")
                            .build());

    private final ScheduledExecutorService pressureMonitorThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("pressure-monitor")
                            .build());


    public RemoteBlockHandler(int localPort, String masterHost, int masterPort, TransportConf conf, File registeredExecutorFile) throws IOException {
        super(conf, registeredExecutorFile);
        this.localPort = localPort;
        this.localHost = findLocalHost();
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.transportConf = conf;
        this.subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories",64);
        this.heartbeatInterval = JavaUtils.timeStringAsSec(conf.get("spark.shuffle.remote.worker.interval","60s"));
        this.monitorInterval = JavaUtils.timeStringAsSec(conf.get("spark.shuffle.remote.worker.monitor","60s"));
        this.mergedDirCleaner = Executors.newSingleThreadExecutor(
                NettyUtils.createThreadFactory("shuffle-merge-worker-directory-cleaner"));
        init();
    }

    private void init() throws IOException {
        TransportContext context = new TransportContext(
                transportConf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        clientFactory = context.createClientFactory(bootstraps);
        try {
            String dirsConfig = transportConf.get("spark.shuffle.remote.worker.dirs", "");
            if (StringUtils.isBlank(dirsConfig)) {
                throw new IOException("Remote shuffle worker dirs is empty");
            } else {
                Arrays.stream(dirsConfig.split(",")).forEach(dir -> {
                    if (dir.contains(":")) {
                        String[] arr = dir.split(":");
                        String path = arr[0];
                        DiskType diskType = DiskType.toDiskType(arr[1]);
                        workDirs.add(new ShuffleDir(path, diskType));
                    } else {
                        workDirs.add(new ShuffleDir(dir, DiskType.HDD));
                    }
                });
                client = clientFactory.createClient(masterHost, masterPort);
                registerRemoteShuffleWorker();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void registerRemoteShuffleWorker() throws InterruptedException, IOException {
        ByteBuffer registerWorker = new RegisterWorker(localHost, localPort).toByteBuffer();
        for (int i = 0; ; i++) {
            try {
                client.sendRpcSync(registerWorker, 3000L);
                heartbeatThread.scheduleAtFixedRate(
                        new Heartbeat(), 10, heartbeatInterval, TimeUnit.SECONDS);

                pressureMonitorThread.scheduleAtFixedRate(
                        new PressureMonitor(), 10, monitorInterval, TimeUnit.SECONDS);
                logger.info("Registered remote shuffle worker successfully");
                return;
            } catch (Exception e) {
                if (i < MAX_ATTEMPTS) {
                    logger.warn("Failed to connect to remote shuffle server, will retry {} more times after waiting 10 seconds...", i-1 ,e);
                    Thread.sleep(10 * 1000L);
                } else {
                    throw new IOException("Unable to register with remote shuffle server due to : " + e.getMessage() , e);
                }
            }
        }
    }

    private void unregisterRemoteShuffleWorker() {
        ByteBuffer unregisterWorker = new UnregisterWorker(localHost, localPort).toByteBuffer();
        client.send(unregisterWorker);
    }


    @Override
    public StreamCallbackWithID receiveStream(
            TransportClient client,
            ByteBuffer messageHeader,
            RpcResponseCallback callback) {
        BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(messageHeader);
        if (msgObj instanceof PushBlockStream) {
            PushBlockStream message = (PushBlockStream) msgObj;
            checkAuth(client, message.appId);
            String appKey = message.appId + "_" + message.appAttemptId;
            String shuffleKey = message.appId + "_" + message.appAttemptId + "_" + message.shuffleId + "_";
            ConcurrentHashMap<String, RunningStage> appRunningStageMap = appStageMap.computeIfAbsent(appKey, v -> {
                registerExecutor(message.appId, message.appAttemptId);
                return new ConcurrentHashMap<>();
            });
            appRunningStageMap.computeIfAbsent(shuffleKey, v ->
                 new RunningStage(
                        message.appId,
                        message.appAttemptId,
                        message.shuffleId
                )
            );
            return mergeManager.receiveBlockDataAsStream(message);
        } else {
            throw new UnsupportedOperationException("Unexpected message with #receiveStream: " + msgObj);
        }
    }

    @Override
    protected void handleMessage(BlockTransferMessage msgObj, TransportClient client, RpcResponseCallback callback) {
        if (msgObj instanceof CleanApplication) {
            CleanApplication cleanApplication = (CleanApplication) msgObj;
            applicationRemoved(cleanApplication.getAppId(), true);
            String appKey = cleanApplication.getAppId() + "_" + cleanApplication.getAttempt();
            appStageMap.remove(appKey);
          List<String> localDirs = localMergeDirs.remove(appKey);
            if (!localDirs.isEmpty()) {
                mergedDirCleaner.execute(() ->
                        deleteExecutorDirs(localDirs, appKey));
            }
        } else if (msgObj instanceof RegisterExecutor) {
            final Timer.Context responseDelayContext =
                    metrics.registerExecutorRequestLatencyMillis.time();
            try {
                RegisterExecutor msg = (RegisterExecutor) msgObj;
                checkAuth(client, msg.appId);
                blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
                // 这里不在使用mergeManager注册工作目录
                callback.onSuccess(ByteBuffer.wrap(new byte[0]));
                logger.info( "Registered executor {} of appId {} with executorInfo {} from host {}",
                        msg.execId,
                        msg.appId,
                        msg.executorInfo.toString(),
                        getRemoteAddress(client.getChannel()));
            } finally {
                responseDelayContext.stop();
            }
        } else {
            super.handleMessage(msgObj, client, callback);
        }
    }

    void deleteExecutorDirs(List<String> localDirs, String appAttempt) {
        Path[] dirs = localDirs.stream().map(dir -> Paths.get(dir)).toArray(Path[]::new);
        for (Path localDir : dirs) {
            try {
                if (Files.exists(localDir)
                        && checkDeleteDirs(localDir.toAbsolutePath().toString(), appAttempt)) {
                    JavaUtils.deleteRecursively(localDir.toFile());
                    logger.info("Successfully cleaned up directory: {}", localDir);
                }
            } catch (Exception e) {
                logger.error("Failed to delete directory: {}", localDir, e);
            }
        }
    }

    public Boolean checkDeleteDirs(String path, String appAttempt) {
        return !StringUtils.isBlank(path) && !StringUtils.isBlank(appAttempt)
                && path.endsWith(appAttempt) ;
    }

    @Override
    public void close() {
        super.close();
        heartbeatThread.shutdownNow();
        if(client != null) {
            unregisterRemoteShuffleWorker();
            client.close();
            client = null;
        }
        if(clientFactory != null) {
            clientFactory.close();
            clientFactory = null;
        }
    }

    private void registerExecutor(String appId, int attemptId) {
        Map<String, String> mergedMetaMap = new HashMap<>();
        mergedMetaMap.put(MERGE_DIR_KEY, "merge_manager_" + attemptId);
        mergedMetaMap.put(ATTEMPT_ID_KEY, String.valueOf(attemptId));
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = null;
        try {
            jsonString = mapper.writeValueAsString(mergedMetaMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String shuffleManagerMeta = "_:" + jsonString; //适配RemoteBlockPushResolver.registerExecutor
        List<String> localDirs = new ArrayList<>();
        List<String> localParentDirs = new ArrayList<>();
        workDirs.forEach(shuffleDir -> {
            String rootDir = shuffleDir.getPath();
            String parentPath = rootDir + "/" + appId + "_" + attemptId;
            localParentDirs.add(parentPath);
            File mergeDir = new File(rootDir, appId + "_" + attemptId + "/merge_manager");
            if(!mergeDir.exists()) {
                for(int dirNum = 0; dirNum < subDirsPerLocalDir; dirNum++) {
                    File subDir = new File(mergeDir, String.format("%02x", dirNum));
                    try {
                        if (!subDir.exists()) {
                            // Only one container will create this directory. The filesystem will handle
                            // any race conditions.
                            createDirWithPermission770(subDir);
                        }
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(mergeDir.exists()){
                localDirs.add(mergeDir.getAbsolutePath());
            }
        });
        String appKey = appId + "_" + attemptId;
        localMergeDirs.computeIfAbsent(appKey, v -> { return localParentDirs; });
        mergeManager.registerExecutor(appId,new ExecutorShuffleInfo(localDirs.toArray(new String[0]), 64, shuffleManagerMeta));
    }

    private void createDirWithPermission770(File dirToCreate) throws IOException, InterruptedException {
        int attempts = 0;
        int maxAttempts = 3;
        File created = null;
        while (created == null) {
            attempts += 1;
            if (attempts > maxAttempts) {
                throw new IOException(
                        "Failed to create directory "+ dirToCreate.getAbsolutePath() +" with permission 770 after 3 attempts!");
            }
            try {
                ProcessBuilder builder = new ProcessBuilder().command(
                        "mkdir", "-p", "-m770", dirToCreate.getAbsolutePath());
                Process proc = builder.start();
                int exitCode = proc.waitFor();
                if (dirToCreate.exists()) {
                    created = dirToCreate;
                }
            } catch (SecurityException e) {
                logger.warn("Failed to create directory " + dirToCreate.getAbsolutePath() + " with permission 770", e);
                created = null;
            }
        }
    }

    private String findLocalHost() throws UnknownHostException {
        String defaultIpOverride = System.getenv("JSS_LOCAL_IP");
        InetAddress local;
        if (defaultIpOverride != null) {
            local = InetAddress.getByName(defaultIpOverride);
        } else {
            local = InetAddress.getLocalHost();
        }
        return local.getHostName();
    }

    private class Heartbeat implements Runnable {
        @Override
        public void run() {
            List<RunningStage> currentRunningStages = new ArrayList<>();
            appStageMap.values().forEach(stageMap-> currentRunningStages.addAll(stageMap.values()));
            logger.info("worker send heartbeat");
            if (!client.isActive()) {
                connection();
            }
            client.send(
                    new RemoteShuffleWorkerHeartbeat(
                            localHost,
                            localPort,
                            System.currentTimeMillis(),
                            "0.0",
                            currentRunningStages.toArray(new RunningStage[0])).toByteBuffer()
            );
        }
    }

    public void connection() {
        TransportContext context = new TransportContext(
                transportConf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        clientFactory = context.createClientFactory(bootstraps);
        try {
            client = clientFactory.createClient(masterHost, masterPort);
        } catch (Exception e) {
          logger.warn("create new client orrcus an new error: ", e.getCause());
        }
    }

    private class PressureMonitor implements Runnable {

        private PressureMonitor() {
        }

        @Override
        public void run() {
            // TODO: 2021/9/26 监控磁盘压力
            logger.info("PressureMonitor");
        }
    }

}
