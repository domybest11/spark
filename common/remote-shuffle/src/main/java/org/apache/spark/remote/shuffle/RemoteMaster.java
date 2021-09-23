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
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.remote.shuffle.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoteMaster {
    private static final Logger logger = LoggerFactory.getLogger(RemoteMaster.class);

    private static RemoteMaster master;
    private static final CountDownLatch barrier = new CountDownLatch(1);

    private int port = 0;
    private TransportContext transportContext;
    private TransportServer server;
    private TransportClientFactory clientFactory;
    private TransportConf transportConf;

    private final ScheduledExecutorService cleanThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("remote-shuffle-master-clean")
                            .build());


    //meta
    public final ConcurrentHashMap<String, WorkerInfo> workersMap = new ConcurrentHashMap<>();
    public final ArrayList<WorkerInfo> blackWorkers = new ArrayList<>();
    public final ArrayList<WorkerInfo> busyWorkers = new ArrayList<>();

    public final ConcurrentHashMap<String, RunningApplication> runningApplicationMap = new ConcurrentHashMap<>();





    private void start() {
        if (server == null) {
            transportContext = new TransportContext(transportConf, new MasterRpcHandler(), true);
            List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
            server = transportContext.createServer(port, bootstraps);
            clientFactory = transportContext.createClientFactory(Lists.newArrayList());
        }
        // application 超时检查
        cleanThread.scheduleAtFixedRate(new ApplicationExpire(), 0, 10, TimeUnit.SECONDS);
    }

    private void stop() {
        if (server != null) {
            server.close();
            server = null;
        }
        if (transportContext != null) {
            transportContext.close();
            transportContext = null;
        }
    }



    public static void main(String[] args) {
        master = new RemoteMaster();
        master.start();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    master.stop();
                    barrier.countDown();
                })
        );
        try {
            barrier.await();
        } catch (InterruptedException e) {

        }
    }


    private class ApplicationExpire implements Runnable {

        @Override
        public void run() {
            Iterator<RunningApplication> it = runningApplicationMap.values().iterator();
            while (it.hasNext()){
                RunningApplication runningApplication = it.next();
                // TODO: 2021/9/22 超时时间
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
                long start = System.currentTimeMillis();
                RegisterWorker registerWorker = (RegisterWorker) msgObj;
                String host = registerWorker.getHost();
                int port = registerWorker.getPort();
                String address = host + ":" + port;
                workersMap.computeIfAbsent(address, w -> new WorkerInfo(clientFactory, host, port));
                callback.onSuccess(ByteBuffer.allocate(0));
                logger.info("handle RegisterWorker time: {}ms", System.currentTimeMillis() - start);
            } else if (msgObj instanceof RemoteShuffleServiceHeartbeat) {
                long start = System.currentTimeMillis();
                RemoteShuffleServiceHeartbeat workHeartbeat = (RemoteShuffleServiceHeartbeat) msgObj;
                String host = workHeartbeat.getHost();
                int port = workHeartbeat.getPort();
                String address = host + ":" + port;
                org.apache.spark.remote.shuffle.RunningStage[] runningStages = workHeartbeat.getRunningStages();
                WorkerPressure pressure = workHeartbeat.getPressure();
                WorkerInfo workerInfo = workersMap.computeIfAbsent(address, w -> new WorkerInfo(clientFactory, host, port));
                workerInfo.setLatestHeartbeatTime(workHeartbeat.getHeartbeatTimeMs());
                workerInfo.setPressure(pressure);
                // TODO: 2021/9/22 根据pressure维护work列表
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
            } else if (msgObj instanceof RegisteredApplication) {
                RegisteredApplication application = (RegisteredApplication) msgObj;
                String appId = application.getAppId();
                int attemptId = application.getAttempt();
                String key = appId + "_" + attemptId;
                RunningApplication runningApplication = runningApplicationMap.computeIfAbsent(key, f -> new RunningApplication(appId,attemptId));
                logger.info("application: {}_{} register success", runningApplication.appId, runningApplication.attemptId);
            } else if (msgObj instanceof UnregisteredApplication) {
                UnregisteredApplication application = (UnregisteredApplication) msgObj;
                String appId = application.getAppId();
                int attemptId = application.getAttempt();
                String key = appId + "_" + attemptId;
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
                long heartbeatTimeoutMs = application.getHeartbeatTimeoutMs();
                String key = appId + "_" + attemptId;
                RunningApplication runningApplication = runningApplicationMap.get(key);
                if (runningApplication != null) {
                    runningApplication.setLatestHeartbeatTime(heartbeatTimeoutMs);
                } else {
                    logger.warn("application: {}_{} not exist in master, restore from driverHeartbeat", appId, attemptId);
                    runningApplicationMap.putIfAbsent(key, new RunningApplication(appId, attemptId));
                }
            } else {
                throw new UnsupportedOperationException("Unexpected message: " + msgObj);
            }
        }

        @Override
        public StreamManager getStreamManager() {
            return null;
        }

    }

    private static class RunningApplication {
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



