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

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.*;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.shuffle.protocol.RegisterExecutor;
import org.apache.spark.network.shuffle.protocol.remote.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.remote.shuffle.metric.IOStatusTracker;
import org.apache.spark.remote.shuffle.metric.NetworkGauge;
import org.apache.spark.remote.shuffle.metric.NetworkTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
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
    private TransportConf transportConf;
    private TransportClientFactory clientFactory;
    private TransportClient client;
    private final int MAX_ATTEMPTS = 3;
    private DiskManager diskManager;
    private final long heartbeatInterval;
    private final long cleanInterval;
    private final WorkerMetrics workerMetrics;

    //meta
    // key: appid_attempt  value: v-key: shuffle_key v-value: runningStage
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, RunningStage>> appStageMap = new ConcurrentHashMap<>();

    private final ScheduledExecutorService heartbeatThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("remote-shuffle-worker-heartbeat")
                            .build());

    private ScheduledExecutorService cleanScheduler =  Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("rss-meta-cleaner")
                    .build());

    public RemoteBlockHandler(int localPort, String masterHost, int masterPort, TransportConf conf, File registeredExecutorFile) throws IOException {
        super(conf, registeredExecutorFile);
        this.localPort = localPort;
        this.localHost = findLocalHost();
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.transportConf = conf;
        this.heartbeatInterval = JavaUtils.timeStringAsSec(conf.get("spark.shuffle.remote.worker.interval", "60s"));
        this.cleanInterval = JavaUtils.timeStringAs(conf.get("spark.shuffle.service.clean.interval", "1h"), TimeUnit.SECONDS);
        this.workerMetrics = new WorkerMetrics();
        init();
    }

    public void checkAndCleanShuffleMeta() {
        long startTime = System.currentTimeMillis();
        Map<String, String[]> appIdToLocalDirs = new HashMap<>();
        int cleanedApps = 0;
        ConcurrentMap<ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo> executors =
                getBlockManager().getExecutors();
        for (Map.Entry<ExternalShuffleBlockResolver.AppExecId, ExecutorShuffleInfo> entry : executors.entrySet()) {
            String appId = entry.getKey().appId;
            String[] localDirs = entry.getValue().localDirs;
            appIdToLocalDirs.putIfAbsent(appId, localDirs);
        }
        for (String appId : appIdToLocalDirs.keySet()) {
            boolean localDirsExists = getBlockManager().checkLocalDirsExists(appIdToLocalDirs.get(appId));
            if (!localDirsExists) {
                logger.info("Cleaning up rss meta for application {}", appId);
                mergeManager.applicationRemoved(appId, true);
                cleanedApps++;
            }
        }
        logger.info("Cleaning up rss meta of {}/{} app(s) cost {} ms", cleanedApps, appIdToLocalDirs.size(),
                System.currentTimeMillis() - startTime);
    }

    private void init() throws IOException {
        TransportContext context = new TransportContext(
                transportConf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        clientFactory = context.createClientFactory(bootstraps);
        try {
            diskManager = new DiskManager(transportConf);
            int monitorInterval = (int) JavaUtils.timeStringAsSec(transportConf.get("spark.shuffle.remote.worker.monitor", "30s"));
            ScheduledExecutorService metricsSampleThread = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("worker-metrics-sample")
                            .build());
            metricsSampleThread.scheduleAtFixedRate(() -> {
                NetworkTracker.collectNetworkInfo(workerMetrics, monitorInterval);
                IOStatusTracker.collectIOInfo(diskManager.workDirs, monitorInterval);
            }, 0, monitorInterval, TimeUnit.SECONDS);
            heartbeatThread.scheduleAtFixedRate(new Heartbeat(), 10, heartbeatInterval, TimeUnit.SECONDS);
            cleanScheduler.scheduleAtFixedRate(this::checkAndCleanShuffleMeta, cleanInterval, cleanInterval, TimeUnit.SECONDS);
            registerRemoteShuffleWorker();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void registerRemoteShuffleWorker() throws InterruptedException, IOException {
        ByteBuffer registerWorker = new RegisterWorker(localHost, localPort).toByteBuffer();
        for (int i = 0; ; i++) {
            try {
                if (null == client || !client.isActive()) {
                    connection();
                }
                client.sendRpcSync(registerWorker, 3000L);
                logger.info("Registered remote shuffle worker successfully");
                return;
            } catch (Exception e) {
                if (i < MAX_ATTEMPTS) {
                    logger.warn("Failed to connect to remote shuffle server, will retry {} more times after waiting 10 seconds...", i - 1, e);
                    Thread.sleep(10 * 1000L);
                } else {
                    logger.warn("Unable to register with remote shuffle server due to: {}", e.getCause());
                    return;
                }
            }
        }
    }

    private void unregisterRemoteShuffleWorker() {
        ByteBuffer unregisterWorker = new UnregisterWorker(localHost, localPort).toByteBuffer();
        if (!client.isActive()) {
            connection();
        }
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
            String shuffleServiceType;
            try {
                shuffleServiceType = message.shuffleServiceType;
            } catch (Exception e) {
                shuffleServiceType = "";
            }
            if (shuffleServiceType.equals("rss_v2")) {
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
            }
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
            mergeManager.applicationRemoved(appKey, true);
            diskManager.cleanApplication(cleanApplication.getAppId(), cleanApplication.getAttempt());
        } else if (msgObj instanceof RegisterExecutor) {
            final Timer.Context responseDelayContext =
                    metrics.registerExecutorRequestLatencyMillis.time();
            try {
                RegisterExecutor msg = (RegisterExecutor) msgObj;
                checkAuth(client, msg.appId);
                String shuffleServiceType;
                try {
                    shuffleServiceType = msg.shuffleServiceType;
                } catch (Exception e) {
                    shuffleServiceType = "";
                }
                if (shuffleServiceType.equals("rss_v1")) {
                    mergeManager.registerExecutor(msg.appId, msg.executorInfo);
                }
                blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
                callback.onSuccess(ByteBuffer.wrap(new byte[0]));
                logger.info("Registered executor {} of appId {} with executorInfo {} from host {}",
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
        if (client != null) {
            unregisterRemoteShuffleWorker();
            client.close();
            client = null;
        }
        if (clientFactory != null) {
            clientFactory.close();
            clientFactory = null;
        }
    }

    private void registerExecutor(String appId, int attemptId) {
        Map<String, String> mergedMetaMap = new HashMap<>();
        mergedMetaMap.put(MERGE_DIR_KEY, "merge_manager");
        mergedMetaMap.put(ATTEMPT_ID_KEY, String.valueOf(attemptId));
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = null;
        try {
            jsonString = mapper.writeValueAsString(mergedMetaMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String shuffleManagerMeta = "_:" + jsonString; //适配RemoteBlockPushResolver.registerExecutor
        String[] mergePaths = diskManager.makeMergeSpace(appId, attemptId);
        logger.info("Choose worker dir is: {}", Arrays.stream(mergePaths).reduce((a,b) -> a + "," + b).get());
        mergeManager.registerExecutor(appId, new ExecutorShuffleInfo(mergePaths, 64, shuffleManagerMeta));
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
            try {
                List<RunningStage> currentRunningStages = new ArrayList<>();
                appStageMap.values().forEach(stageMap -> currentRunningStages.addAll(stageMap.values()));
                logger.info("worker send heartbeat");
                if (null == client || !client.isActive()) {
                    connection();
                }
                long[] workerMetric = workerMetrics.getCurrentMetrics();
                logger.info("worker pressure: {}", workerMetric);
                client.send(
                    new RemoteShuffleWorkerHeartbeat(
                        localHost,
                        localPort,
                        System.currentTimeMillis(),
                        workerMetric,
                        currentRunningStages.toArray(new RunningStage[0])
                    ).toByteBuffer());
            } catch (Exception e) {
                    logger.warn("Unable to send heartbeat to remote shuffle server due to: {}", e.getCause());
            }
        }
    }


    public void connection() {
        try {
            TransportContext context = new TransportContext(
                    transportConf, new NoOpRpcHandler(), true, true);
            List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
            clientFactory = context.createClientFactory(bootstraps);
            client = clientFactory.createClient(masterHost, masterPort);
        } catch (Exception e) {
            logger.warn("create new client orrcus an new error: ", e.getCause());
        }
    }

    public class WorkerMetrics {
        OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
        // CPU
        private final Gauge<Long> workerCpuLoadAverage = () -> (long) osmxb.getSystemLoadAverage();
        private final Gauge<Long> workerCpuAvailable = () -> (long) osmxb.getAvailableProcessors();

        // Network
        public final NetworkGauge networkInGauge = new NetworkGauge(4);
        public final NetworkGauge networkOutGauge = new NetworkGauge(4);
        public final Meter workerNetworkIn = new Meter();
        public final Meter workerNetworkOut = new Meter();

        // Connection
        private final Counter workerAliveConnection = metrics.activeConnections;

        public long[] getCurrentMetrics() {
            int diskNums = diskManager.workDirs.length;
            long[] metrics = new long[7 + diskNums * 9 + 1];
            metrics[0] = workerCpuLoadAverage.getValue();
            metrics[1] = workerCpuAvailable.getValue();
            metrics[2] = networkInGauge.getValue();
            metrics[3] = (long) workerNetworkIn.getFiveMinuteRate();
            metrics[4] = networkOutGauge.getValue();
            metrics[5] = (long) workerNetworkOut.getFiveMinuteRate();
            metrics[6] = workerAliveConnection.getCount();
            for (int i = 0; i < diskNums; i++) {
                DiskInfo.DiskMetrics diskMetrics = diskManager.workDirs[i].diskMetrics;
                metrics[7 + i * 9] = diskMetrics.diskReadsCompleted.getValue();
                metrics[8 + i * 9] = (long) diskMetrics.diskRead.getFiveMinuteRate();
                metrics[9 + i * 9] = diskMetrics.diskWritesCompleted.getValue();
                metrics[10 + i * 9] = (long) diskMetrics.diskWrite.getFiveMinuteRate();
                metrics[11 + i * 9] = diskMetrics.diskIOTime.getValue();
                metrics[12 + i * 9] = (long) diskMetrics.diskUtils.getFiveMinuteRate();
                metrics[13 + i * 9] = diskMetrics.diskSpaceUsed.getValue();
                metrics[14 + i * 9] = diskMetrics.diskInodeAvailable.getValue();
                metrics[15 + i * 9] = diskManager.workDirs[i].getType().ordinal();
            }
            metrics[7 + diskNums * 9] = diskNums;
            return metrics;
        }

        public String[] metricGetters = {
                "workerCpuLoadAverage",
                "workerCpuAvailable",
                "workerNetworkIn",
                "workerNetworkIn_5min",
                "workerNetworkOut",
                "workerNetworkOut_5min",
                "workerAliveConnection"
        };
    }

}
