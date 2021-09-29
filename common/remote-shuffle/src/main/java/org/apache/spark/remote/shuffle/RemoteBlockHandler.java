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
import org.apache.spark.network.shuffle.protocol.remote.RunningStage;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.shuffle.protocol.remote.CleanApplication;
import org.apache.spark.network.shuffle.protocol.remote.RegisterWorker;
import org.apache.spark.network.shuffle.protocol.remote.RemoteShuffleServiceHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

// remote worker
public class RemoteBlockHandler extends ExternalBlockHandler {
    private static final Logger logger = LoggerFactory.getLogger(RemoteBlockHandler.class);
    private final String masterHost;
    private final int masterPort;
    private final String localHost;
    private final int localPort;
    private volatile TransportClientFactory clientFactory;
    private final TransportConf transportConf;
    private final int heartbeatInterval = 1;
    private final int monitorInterval = 1;


    //meta
//    private ConcurrentHashMap<String, ShuffleDir> workDirs = new ConcurrentHashMap<>();
    private List<ShuffleDir> workDirs = new ArrayList<>();
    // key: appid_attempt  value: v-key: shuffle_key v-value: runningStage
    private ConcurrentHashMap<String, ConcurrentHashMap<String, RunningStage>> appStageMap = new ConcurrentHashMap<>();
//    private ConcurrentHashMap<ShuffleDir,List<RunningStage>> dirInfo = new ConcurrentHashMap<>();


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
        init();
    }

    private void init() throws IOException {
        TransportContext context = new TransportContext(
                transportConf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        clientFactory = context.createClientFactory(bootstraps);
        try {
            String dirsConfig = transportConf.get("shuffle.remote.worker.dirs", "");
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
                registerRemoteShuffleWorker();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void registerRemoteShuffleWorker() throws IOException, InterruptedException {
        TransportClient client = clientFactory.createClient(masterHost, masterPort);
        ByteBuffer registerWorker = new RegisterWorker(localHost, localPort).toByteBuffer();
        client.sendRpc(registerWorker, new RegisterWorkerCallback(client));
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
            // TODO: 2021/9/26 第一次push数据,需要构建相关的数据结构, 构建过程最好可以提前, push开始后性能非常重要
            //  原实现时工作目录通过RegisterExecutor注册, rss无法这样注册
            //  个人想法
            //  1：registerShuffle的时候就开始location获取，同时相关worker就开始构造相关的工作目录
            //  2.driver在DagScheduler.prepareShuffleServicesForShuffleMapStage 获取到目标worker后，这
            //  时候driver开始向worker申请工作目录
            
            ConcurrentHashMap<String, RunningStage> appRunningStageMap = appStageMap.computeIfAbsent(appKey, v -> {
                prepareAppWorkDir(message.appId, message.appAttemptId);
                return new ConcurrentHashMap<>();
            });
            RunningStage runningStage = appRunningStageMap.computeIfAbsent(shuffleKey, v -> 
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
            // TODO: 2021/9/23 这里有个问题，这种直接删除会影响到application attempt
            applicationRemoved(cleanApplication.getAppId(), true);
            String appKey = cleanApplication.getAppId() + "_" + cleanApplication.getAttempt();
            ConcurrentHashMap<String, RunningStage> map =  appStageMap.remove(appKey);
            if (map != null) {
                RunningStage[] stages = map.values().toArray(new RunningStage[0]);
            }
        } else if (msgObj instanceof RegisterExecutor) {
            final Timer.Context responseDelayContext =
                    metrics.registerExecutorRequestLatencyMillis.time();
            try {
                RegisterExecutor msg = (RegisterExecutor) msgObj;
                checkAuth(client, msg.appId);
                blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
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

    @Override
    public void close() {
        super.close();
        heartbeatThread.shutdownNow();
    }


    private void prepareAppWorkDir(String appId, int attemptId) {
        // TODO: 2021/9/26 兼容调社区版本注册app工作目录
        workDirs.forEach(shuffleDir -> {
            String rootDir = shuffleDir.getPath();
            File mergeDir = new File(rootDir, appId+"/_"+attemptId+"/merge_manager");
            if (!mergeDir.exists()) {
                for(int dirNum=0; dirNum<64; dirNum++){
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
            mergeManager.registerExecutor(appId,new ExecutorShuffleInfo(null,64,null));
        });
    }

    private void createDirWithPermission770(File dirToCreate) throws IOException, InterruptedException {
        int attempts = 0;
        int maxAttempts = 10;
        File created = null;
        while (created == null) {
            attempts += 1;
            if (attempts > maxAttempts) {
                throw new IOException(
                        "Failed to create directory "+ dirToCreate.getAbsolutePath() +" with permission 770 after $maxAttempts attempts!");
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


    private class RegisterWorkerCallback implements RpcResponseCallback {
        private final TransportClient client;

        public RegisterWorkerCallback(TransportClient client) {
            this.client = client;
        }

        @Override
        public void onFailure(Throwable e) {
            logger.error("Unable to registered remote shuffle worker");
            System.exit(-1);
        }

        @Override
        public void onSuccess(ByteBuffer response) {
            heartbeatThread.scheduleAtFixedRate(
                    new Heartbeat(client), 1, heartbeatInterval, TimeUnit.SECONDS);

            pressureMonitorThread.scheduleAtFixedRate(
                    new PressureMonitor(), 1, monitorInterval, TimeUnit.SECONDS);

            logger.info("Successfully registered remote shuffle worker");
        }
    }

    private class Heartbeat implements Runnable {
        private final TransportClient client;

        private Heartbeat(TransportClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            List<RunningStage> currentRunningStages = new ArrayList<>();
            appStageMap.values().forEach(stageMap-> currentRunningStages.addAll(stageMap.values()));
            // TODO: 2021/9/22 心跳上报
            client.send(
                    new RemoteShuffleServiceHeartbeat(
                            "host",
                            0,
                            System.currentTimeMillis(),
                            "0.0",
                            currentRunningStages.toArray(new RunningStage[0])).toByteBuffer()
            );
        }
    }

    private class PressureMonitor implements Runnable {

        private PressureMonitor() {
        }

        @Override
        public void run() {
            // TODO: 2021/9/26 监控磁盘压力
        }
    }

}
