package org.apache.spark.remote.shuffle;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.remote.*;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RemoteShuffleMasterHandler {
    private static final Logger logger = LoggerFactory.getLogger(RemoteShuffleMasterHandler.class);

    private String host;
    private int port;
    private TransportConf transportConf;
    private TransportContext transportContext;
    private TransportServer server;
    private TransportClientFactory clientFactory;

    private final ScheduledExecutorService cleanThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("remote-shuffle-master-clean")
                            .build());


    //meta
    public final ConcurrentHashMap<String, WorkerInfo> workersMap = new ConcurrentHashMap<>();
    public final HashSet<WorkerInfo> blackWorkers = new HashSet<>();
    public final HashSet<WorkerInfo> busyWorkers = new HashSet<>();

    public final ConcurrentHashMap<String, RunningApplication> runningApplicationMap = new ConcurrentHashMap<>();

    public RemoteShuffleMasterHandler(String host, int port, TransportConf conf) {
        this.host = host;
        this.port = port;
        transportConf = conf;
    }

    public void start() {
        if (server == null) {
            transportContext = new TransportContext(transportConf, new MasterRpcHandler(), true);
            List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
            server = transportContext.createServer(port, bootstraps);
            clientFactory = transportContext.createClientFactory(Lists.newArrayList());
        }
        // application 超时检查
        cleanThread.scheduleAtFixedRate(new ApplicationExpire(), 0, 1, TimeUnit.MINUTES);
    }

    public void stop() {
        if (server != null) {
            server.close();
            server = null;
        }
        if (transportContext != null) {
            transportContext.close();
            transportContext = null;
        }
    }


    private class ApplicationExpire implements Runnable {

        @Override
        public void run() {
            Iterator<RunningApplication> it = runningApplicationMap.values().iterator();
            while (it.hasNext()){
                RunningApplication runningApplication = it.next();
                // TODO: 2021/9/22 application 超时过期回收
                if (runningApplication.latestHeartbeatTime + 1000L > System.currentTimeMillis()) {
                    it.remove();
                    runningApplication.alive.compareAndSet(true, false);
                    synchronized (runningApplication) {
                        runningApplication.workerInfos.forEach(workerInfo ->
                                workerInfo.cleanApplication(runningApplication.appId, runningApplication.attemptId)
                        );
                    }
                }
            }
        }
    }


    private class MasterRpcHandler extends RpcHandler {

        @Override
        public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
            BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
            handleMessage(msgObj, client, callback);
        }

        private void handleMessage(BlockTransferMessage msgObj, TransportClient client, RpcResponseCallback callback) {
            if (msgObj instanceof RegisterWorker) {
                // TODO: 2021/9/26 注册worker
                long start = System.currentTimeMillis();
                RegisterWorker registerWorker = (RegisterWorker) msgObj;
                String host = registerWorker.getHost();
                int port = registerWorker.getPort();
                String address = host + ":" + port;
                workersMap.computeIfAbsent(address, w -> new WorkerInfo(clientFactory, host, port));
                callback.onSuccess(ByteBuffer.allocate(0));
                logger.info("handle RegisterWorker time: {}ms", System.currentTimeMillis() - start);
            } else if (msgObj instanceof RemoteShuffleServiceHeartbeat) {
                // TODO: 2021/9/26 worker 心跳处理
                long start = System.currentTimeMillis();
                RemoteShuffleServiceHeartbeat workHeartbeat = (RemoteShuffleServiceHeartbeat) msgObj;
                String host = workHeartbeat.getHost();
                int port = workHeartbeat.getPort();
                String address = host + ":" + port;
                RunningStage[] runningStages = workHeartbeat.getRunningStages();
                WorkerPressure pressure = new WorkerPressure(workHeartbeat.getPressure());
                WorkerInfo workerInfo = workersMap.computeIfAbsent(address, w -> new WorkerInfo(clientFactory, host, port));
                workerInfo.setLatestHeartbeatTime(workHeartbeat.getHeartbeatTimeMs());
                workerInfo.setPressure(pressure);
                // TODO: 2021/9/22 根据pressure维护work列, 部分不健康的节点从可用节点
                if ( pressure != null) {

                }
                Arrays.stream(runningStages).forEach(runningStage -> {
                    String appId = runningStage.getApplicationId();
                    int attemptId = runningStage.getAttemptId();
                    String key = appId + "_" + attemptId;
                    RunningApplication runningApplication = runningApplicationMap.computeIfAbsent(key, f -> new RunningApplication(appId,attemptId));
                    synchronized (runningApplication) {
                        runningApplication.getWorkerInfos().add(workerInfo);
                    }
                });
                logger.info("handle RemoteShuffleServiceHeartbeat time: {}ms", System.currentTimeMillis() - start);
            } else if (msgObj instanceof RegisterApplication) {
                RegisterApplication application = (RegisterApplication) msgObj;
                String appId = application.getAppId();
                int attemptId = application.getAttempt();
                String key = application.getKey();
                RunningApplication runningApplication = runningApplicationMap.computeIfAbsent(key, f -> new RunningApplication(appId, attemptId));
                logger.info("application: {}_{} register success", runningApplication.appId, runningApplication.attemptId);
            } else if (msgObj instanceof UnregisterApplication) {
                UnregisterApplication application = (UnregisterApplication) msgObj;
                String appId = application.getAppId();
                int attemptId = application.getAttempt();
                String key = application.getKey();
                RunningApplication runningApplication = runningApplicationMap.remove(key);
                if (runningApplication != null) {
                    runningApplication.alive.compareAndSet(true, false);
                    runningApplication.workerInfos.forEach(
                            workerInfo -> workerInfo.cleanApplication(appId, attemptId)
                    );
                }
            } else if (msgObj instanceof RemoteShuffleDriverHeartbeat) {
                RemoteShuffleDriverHeartbeat application = (RemoteShuffleDriverHeartbeat) msgObj;
                String appId = application.getAppId();
                int attemptId = application.getAttempt();
                long heartbeatTimeoutMs = application.getLatestHeartbeatTime();
                String key = application.getKey();
                RunningApplication runningApplication = runningApplicationMap.get(key);
                if (runningApplication != null) {
                    runningApplication.setLatestHeartbeatTime(heartbeatTimeoutMs);
                } else {
                    logger.warn("application: {}_{} not exist in master, restore from driverHeartbeat", appId, attemptId);
                    runningApplicationMap.putIfAbsent(key, new RunningApplication(appId, attemptId));
                }
            }  else if (msgObj instanceof GetPushMergerLocations) {
                GetPushMergerLocations msg = (GetPushMergerLocations) msgObj;
                String appId = msg.getAppId();
                int attempt = msg.getAttempt();
                int shuffleId = msg.getShuffleId();
                int numPartitions = msg.getNumPartitions();
                int tasksPerExecutor = msg.getTasksPerExecutor();
                int maxExecutors = msg.getMaxExecutors();
                List<WorkerInfo> workerInfos = getMergerWorkers(appId, attempt, shuffleId, numPartitions, tasksPerExecutor, maxExecutors);
                String[] res = new String[workerInfos.size()];
                for (int i=0; i < workerInfos.size(); i++) {
                    res[i] = workerInfos.get(i).address();
                }
                callback.onSuccess(new MergerWorkers(res).toByteBuffer());
                logger.info( "Get push merger locations for app {} shuffle {} ", appId, shuffleId);
            } else {
                throw new UnsupportedOperationException("Unexpected message: " + msgObj);
            }
        }

        @Override
        public StreamManager getStreamManager() {
            return null;
        }

        private List<WorkerInfo> getMergerWorkers(String appId, int attempt, int shuffleId, int numPartitions, int tasksPerExecutor, int maxExecutors) {
            int numMergersDesired = Math.min(Math.max(1, (int)Math.ceil(numPartitions * 1.0 / tasksPerExecutor)), maxExecutors);
            List<WorkerInfo> workerInfos = workersMap.values().stream().filter(
                    workerInfo ->
                            !blackWorkers.contains(workerInfo) && !busyWorkers.contains(workerInfo)
            ).collect(Collectors.toList());
            int capacity = workerInfos.size();
          if (numMergersDesired <= capacity) {
                Collections.shuffle(workerInfos);
                return workerInfos.subList(0, numMergersDesired);
            } else {
                return workerInfos;
            }
        }
    }

    public static class RunningApplication {
        private String appId;
        private int attemptId;
        private Set<WorkerInfo> workerInfos = new HashSet<>();
        private long latestHeartbeatTime;
        private AtomicBoolean alive;

        public RunningApplication(String appId, int attemptId) {
            this.appId = appId;
            this.attemptId = attemptId;
            alive = new AtomicBoolean(true);
            latestHeartbeatTime = System.currentTimeMillis();
        }

        public String getAppId() {
            return appId;
        }

        public void setAppId(String appId) {
            this.appId = appId;
        }

        public int getAttemptId() {
            return attemptId;
        }

        public void setAttemptId(int attemptId) {
            this.attemptId = attemptId;
        }

        public Set<WorkerInfo> getWorkerInfos() {
            return workerInfos;
        }

        public void setWorkerInfos(Set<WorkerInfo> workerInfos) {
            this.workerInfos = workerInfos;
        }

        public long getLatestHeartbeatTime() {
            return latestHeartbeatTime;
        }

        public void setLatestHeartbeatTime(long latestHeartbeatTime) {
            if(latestHeartbeatTime > this.latestHeartbeatTime) {
                this.latestHeartbeatTime = latestHeartbeatTime;
            }
        }
    }

}



