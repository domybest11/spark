package org.apache.spark.remote.shuffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.remote.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LevelDBProvider;
import org.apache.spark.network.util.TransportConf;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

    private DB db;
    private static final LevelDBProvider.StoreVersion CURRENT_VERSION = new LevelDBProvider.StoreVersion(1, 0);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final long applicationExpireTimeout;

    private final ScheduledExecutorService cleanThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("remote-shuffle-master-clean")
                            .build());


    //meta
    public final ConcurrentHashMap<String, WorkerInfo> workersMap = new ConcurrentHashMap<>();
    public final CopyOnWriteArraySet<WorkerInfo> blackWorkers = new CopyOnWriteArraySet<>();
    public final CopyOnWriteArraySet<WorkerInfo> busyWorkers = new CopyOnWriteArraySet<>();

    public final ConcurrentHashMap<String, RunningApplication> runningApplicationMap = new ConcurrentHashMap<>();
    private static final String RUNNING_APP_PREFIX = "RunningApplication";
    private static final String WORKERS_PREFIX = "Workers";
    private static final String BLACK_WORKERS_PREFIX = "BlackWorkers";
    private static final String BUSY_WORKERS_PREFIX = "BusyWorkers";

    public RemoteShuffleMasterHandler(String host, int port, TransportConf conf) throws IOException {
        this.host = host;
        this.port = port;
        transportConf = conf;
        this.applicationExpireTimeout = JavaUtils.timeStringAsSec(conf.get("spark.shuffle.remote.application.expire","600s")) * 1000;
        String filePath = conf.get("spark.shuffle.master.recovery.path", null);
        if (filePath != null) {
            File file = new File(filePath);
            try {
                db = LevelDBProvider.initLevelDB(file, CURRENT_VERSION, mapper);
            } catch (Exception e) {
                db = null;
            }
        }
        if (db != null) {
            reloadRemoteMasterStateFromDB(db);
        }
    }

    private void reloadRemoteMasterStateFromDB(DB db) throws IOException {
        if (db != null) {
            DBIterator itr = db.iterator();
            while (itr.hasNext()) {
                Map.Entry<byte[], byte[]> e = itr.next();
                String key = new String(e.getKey(), StandardCharsets.UTF_8);
                String keyType = getDBType(key);
                switch (keyType) {
                    case RUNNING_APP_PREFIX:
                        String appId = parseDbKey(key);
                        logger.info("Reloading registered runningApp: " +  appId);
                        RunningApplication runningApplication = mapper.readValue(e.getValue(), RunningApplication.class);
                        runningApplicationMap.put(appId, runningApplication);
                        break;
                    case WORKERS_PREFIX:
                        String address = parseDbKey(key);
                        logger.info("Reloading registered worker: " +  address);
                        WorkerInfo workerInfo = mapper.readValue(e.getValue(), WorkerInfo.class);
                        workersMap.put(address, workerInfo);
                        break;
                    case BLACK_WORKERS_PREFIX:
                        String blackWorker = parseDbKey(key);
                        logger.info("Reloading registered blackWorker: " + blackWorker);
                        WorkerInfo blackWorkerInfo = mapper.readValue(e.getValue(), WorkerInfo.class);
                        blackWorkers.add(blackWorkerInfo);
                        break;
                    case BUSY_WORKERS_PREFIX:
                        String busyWorker = parseDbKey(key);
                        logger.info("Reloading registered busyWorker: " + busyWorker);
                        WorkerInfo busyWorkerInfo = mapper.readValue(e.getValue(), WorkerInfo.class);
                        busyWorkers.add(busyWorkerInfo);
                        break;
                    default:
                }
            }
        }
    }

    public String getDBType(String key) {
      if (key.startsWith(RUNNING_APP_PREFIX)) {
          return RUNNING_APP_PREFIX;
      }
      if (key.startsWith(WORKERS_PREFIX)) {
          return WORKERS_PREFIX;
      }
      if (key.startsWith(BLACK_WORKERS_PREFIX)) {
            return BLACK_WORKERS_PREFIX;
      }
      if (key.startsWith(BUSY_WORKERS_PREFIX)) {
            return BUSY_WORKERS_PREFIX;
      }
      return key;
    }

    public String parseDbKey(String key) throws IOException {
        String keyType = getDBType(key);
        String result = "";
        switch (keyType) {
            case RUNNING_APP_PREFIX:
                String appJson = key.substring(RUNNING_APP_PREFIX.length() + 1);
                result = mapper.readValue(appJson, String.class);
                break;
            case WORKERS_PREFIX:
                String workersJson = key.substring(WORKERS_PREFIX.length() + 1);
                result = mapper.readValue(workersJson, String.class);
                break;
            case BLACK_WORKERS_PREFIX:
                String blackWorkerJson = key.substring(BLACK_WORKERS_PREFIX.length() + 1);
                result = mapper.readValue(blackWorkerJson, String.class);
                break;
            case BUSY_WORKERS_PREFIX:
                String busyWorkerJson = key.substring(BUSY_WORKERS_PREFIX.length() + 1);
                result = mapper.readValue(busyWorkerJson, String.class);
                break;
            default:
        }
        return result;
    }

    public byte[] dbAppExecKey(String indexName, Object keyInfo) throws IOException {
        String keyType = getDBType(indexName);
        String appExecJson = mapper.writeValueAsString(keyInfo);
        String key = (keyType + ";" + appExecJson);
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public void saveMasterStateDB(Object keyInfo, Object obj, String indexName) {
        try {
            if (db != null) {
                byte[] key = dbAppExecKey(indexName, keyInfo);
                byte[] value = mapper.writeValueAsString(obj).getBytes(StandardCharsets.UTF_8);
                db.put(key, value);
            }
        } catch (Exception e) {
            logger.error("Error saving registered " + indexName, e);
        }
    }

    public void deleteMasterStateDB(Object keyInfo, String indexName) {
        try {
            if (db != null) {
                byte[] key = dbAppExecKey(indexName, keyInfo);
                db.delete(key);
            }
        } catch (Exception e) {
            logger.error("Error deleting registered " + indexName, e);
        }
    }

    public void start() {
        if (server == null) {
            transportContext = new TransportContext(transportConf, new MasterRpcHandler(), true);
            List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
            server = transportContext.createServer(port, bootstraps);
            clientFactory = transportContext.createClientFactory(Lists.newArrayList());
        }
        // application 超时检查
        cleanThread.scheduleAtFixedRate(new ApplicationExpire(), 1, 1, TimeUnit.MINUTES);
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
                if (runningApplication.latestHeartbeatTime + applicationExpireTimeout > System.currentTimeMillis()) {
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
                workersMap.computeIfAbsent(address, w -> {
                  WorkerInfo workerInfo = new WorkerInfo(clientFactory, host, port);
                  saveMasterStateDB(address, workerInfo, WORKERS_PREFIX);
                  return workerInfo;
                });
                callback.onSuccess(ByteBuffer.allocate(0));
                logger.info("handle register worker time: {}ms", System.currentTimeMillis() - start);
            } else if (msgObj instanceof RemoteShuffleWorkerHeartbeat) {
                logger.info("handle RemoteShuffleServiceHeartbeat");
                long start = System.currentTimeMillis();
                RemoteShuffleWorkerHeartbeat workHeartbeat = (RemoteShuffleWorkerHeartbeat) msgObj;
                String host = workHeartbeat.getHost();
                int port = workHeartbeat.getPort();
                String address = host + ":" + port;
                RunningStage[] runningStages = workHeartbeat.getRunningStages();
                WorkerPressure pressure = new WorkerPressure(workHeartbeat.getPressure());
                WorkerInfo workerInfo = workersMap.computeIfAbsent(address, w -> {
                    WorkerInfo worker = new WorkerInfo(clientFactory, host, port);
                    saveMasterStateDB(address, worker, WORKERS_PREFIX);
                    return worker;
                });
                workerInfo.setLatestHeartbeatTime(workHeartbeat.getHeartbeatTimeMs());
                workerInfo.setPressure(pressure);
                // TODO: 2021/9/22 根据pressure维护work列, 部分不健康的节点从可用节点
                if ( pressure != null) {

                }
                Arrays.stream(runningStages).forEach(runningStage -> {
                    String appId = runningStage.getApplicationId();
                    int attemptId = runningStage.getAttemptId();
                    String key = appId + "_" + attemptId;
                    RunningApplication runningApplication = runningApplicationMap.computeIfAbsent(key, f -> new RunningApplication(appId, attemptId));
                    synchronized (runningApplication) {
                        runningApplication.getWorkerInfos().add(workerInfo);
                    }
                });
                logger.info("handle RemoteShuffleServiceHeartbeat time: {}ms", System.currentTimeMillis() - start);
            } else if (msgObj instanceof UnregisterWorker) {
                UnregisterWorker unregisterWorker = (UnregisterWorker) msgObj;
                String host = unregisterWorker.getHost();
                int port = unregisterWorker.getPort();
                String address = host + ":" + port;
                deleteMasterStateDB(address, WORKERS_PREFIX);
                WorkerInfo workerInfo = workersMap.remove(address);
                blackWorkers.remove(workerInfo);
                busyWorkers.remove(workerInfo);
            } else if (msgObj instanceof RegisterApplication) {
                RegisterApplication application = (RegisterApplication) msgObj;
                String appId = application.getAppId();
                int attemptId = application.getAttempt();
                String key = application.getKey();
                RunningApplication runningApplication = runningApplicationMap.computeIfAbsent(key, f -> new RunningApplication(appId, attemptId));
                callback.onSuccess(ByteBuffer.wrap(new byte[0]));
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
                logger.info( "Get push merger locations for app {} shuffle {} {}", appId, shuffleId, String.join(",", res));
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



