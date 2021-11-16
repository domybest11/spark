package org.apache.spark.remote.shuffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.ArrayUtils;
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
    private TransportConf conf;
    private TransportContext transportContext;
    private TransportServer server;
    private TransportClientFactory clientFactory;
    private Double busyScore;
    private Double blackScore;
    private int wellTime;
    private Double diskIoUtilThreshold;
    private Double diskSpaceThreshold;
    private Double diskInodeThreshold;

    public DB db;
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
    public static final String RUNNING_APP_PREFIX = "RunningApplication";
    public static final String WORKERS_PREFIX = "Workers";
    public static final String BLACK_WORKERS_PREFIX = "BlackWorkers";
    public static final String BUSY_WORKERS_PREFIX = "BusyWorkers";

    public RemoteShuffleMasterHandler(String host, int port, TransportConf conf) throws IOException {
        this.host = host;
        this.port = port;
        this.conf = conf;
        this.applicationExpireTimeout = JavaUtils.timeStringAsSec(conf.get("spark.shuffle.remote.application.expire","600s")) * 1000;
        this.busyScore = Double.parseDouble(conf.get("spark.shuffle.worker.busyScore","0.8"));
        this.blackScore = Double.parseDouble(conf.get("spark.shuffle.worker.blackScore","0.9"));
        this.wellTime = Integer.parseInt(conf.get("spark.shuffle.worker.wellTime","5"));
        this.diskIoUtilThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskIOUtils","0.9"));
        this.diskSpaceThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskSpace","0.1"));
        this.diskInodeThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskInode","0.1"));
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

    public void reloadRemoteMasterStateFromDB(DB db) throws IOException {
        if (db != null) {
            DBIterator itr = db.iterator();
            for (itr.seekToFirst(); itr.hasNext();) {
                Map.Entry<byte[], byte[]> e = itr.next();
                String key = new String(e.getKey(), StandardCharsets.UTF_8);
                String keyType = getDBType(key);
                switch (keyType) {
                    case RUNNING_APP_PREFIX:
                        String appId = parseDbKey(key);
                        logger.info("Reloading registered runningApp: " +  appId);
                        RunningApplication runningApplication = mapper.readValue(e.getValue(), RunningApplication.class);
                        runningApplication.getWorkerInfos().forEach(worker -> {
                            worker.setClientFactory(clientFactory);
                        });
                        runningApplicationMap.put(appId, runningApplication);
                        break;
                    case WORKERS_PREFIX:
                        String address = parseDbKey(key);
                        logger.info("Reloading registered worker: " +  address);
                        WorkerInfo workerInfo = mapper.readValue(e.getValue(), WorkerInfo.class);
                        workerInfo.setClientFactory(clientFactory);
                        workersMap.put(address, workerInfo);
                        break;
                    case BLACK_WORKERS_PREFIX:
                        String blackWorker = parseDbKey(key);
                        logger.info("Reloading registered blackWorker: " + blackWorker);
                        WorkerInfo blackWorkerInfo = mapper.readValue(e.getValue(), WorkerInfo.class);
                        blackWorkerInfo.setClientFactory(clientFactory);
                        blackWorkers.add(blackWorkerInfo);
                        break;
                    case BUSY_WORKERS_PREFIX:
                        String busyWorker = parseDbKey(key);
                        logger.info("Reloading registered busyWorker: " + busyWorker);
                        WorkerInfo busyWorkerInfo = mapper.readValue(e.getValue(), WorkerInfo.class);
                        busyWorkerInfo.setClientFactory(clientFactory);
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
            transportContext = new TransportContext(conf, new MasterRpcHandler(), true);
            List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
            server = transportContext.createServer(port, bootstraps);
            clientFactory = transportContext.createClientFactory(Lists.newArrayList());
        }
        // application 超时检查
        cleanThread.scheduleAtFixedRate(new ApplicationExpire(), 1, 1, TimeUnit.MINUTES);
    }

    public void stop() {
        logger.info("Remote master server is stopping");
        if (server != null) {
            server.close();
            server = null;
        }
        if (transportContext != null) {
            transportContext.close();
            transportContext = null;
        }
        if (db != null) {
            try {
                db.close();
            } catch (IOException e) {
                logger.error("levelDB encountered an error during shutdown:", e.getCause());
            }
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


    public class MasterRpcHandler extends RpcHandler {

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
                WorkerPressure pressure = new WorkerPressure(workHeartbeat.getWorkerMetric());
                WorkerInfo workerInfo = workersMap.compute(address, (id, w) -> {
                 if (w != null) {
                     double score = computeWorkerScore(pressure, w.isShortTime(System.currentTimeMillis()));
                     LinkedList<Double> historyScores = w.historyScores;
                     int size = historyScores.size();
                     if (size > wellTime) {
                         historyScores.peekFirst();
                     }
                     historyScores.add(score);
                     w.setScore(score);
                     w.setLatestHeartbeatTime(workHeartbeat.getHeartbeatTimeMs());
                     w.setPressure(pressure);
                     saveMasterStateDB(address, w, WORKERS_PREFIX);
                     return w;
                 } else {
                     double score = computeWorkerScore(pressure, true);
                     WorkerInfo worker = new WorkerInfo(clientFactory, host, port);
                     worker.setScore(score);
                     worker.historyScores.add(score);
                     worker.setLatestHeartbeatTime(workHeartbeat.getHeartbeatTimeMs());
                     worker.setPressure(pressure);
                     saveMasterStateDB(address, worker, WORKERS_PREFIX);
                     return worker;
                 }
                });
                updateBusyAndBlackWorkers(workerInfo);
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
                    worker -> !busyWorkers.contains(worker) || !blackWorkers.contains(worker)
            ).collect(Collectors.toList());
            int capacity = workerInfos.size();
          if (numMergersDesired <= capacity) {
                Collections.shuffle(workerInfos);
                List<WorkerInfo> targetWorkers = workerInfos.subList(0, numMergersDesired);
                List<WorkerInfo> busyWorkers = workersMap.values().stream()
                        .filter(worker -> workerInfos.contains(worker) && filterBadGagueWorker(worker))
                        .collect(Collectors.toList());
                long delta = targetWorkers.size() - busyWorkers.size();
                if (delta > 0) {
                    targetWorkers.removeAll(busyWorkers);
                    Iterator<WorkerInfo> iterator = workersMap.values().iterator();
                    int count = 0;
                    while (iterator.hasNext() && count < delta) {
                        WorkerInfo worker = iterator.next();
                        if (!busyWorkers.contains(worker) && !filterBadGagueWorker(worker)) {
                            targetWorkers.add(worker);
                            count++;
                        }
                    }
                }
                return targetWorkers;
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

        public RunningApplication() {
        }

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

    public enum WorkerMertric {

        CPULOADAVERAGE("cpuLoadAverage", 0.5, 0.0, 100.0),
        CPUAVAILABLE("cpuLoadAverage", 0.5, 0.0, 100.0),
        NETWORKINBYTES("networkInBytes", 0.5, 0.0, 100.0),
        NETWORKINBYTES5MIN("networkInBytes5min", 0.5, 0.0, 100.0),
        NETWORKOUTBYTES("networkOutBytes", 0.5, 0.0, 100.0),
        NETWORKOUTBYTES5MIN("networkInBytes5min", 0.5, 0.0, 100.0),
        ALIVECONNECTION("aliveConnection", 0.5, 0.0, 100.0),
        DISKREAD("diskRead", 0.5, 0.0, 100.0),
        DISKREAD5MIN("diskRead5min", 0.5, 0.0, 100.0),
        DISKWRITE("diskWrite", 0.5, 0.0, 100.0),
        DISKWRITE5MIN("diskWrite5min", 0.5, 0.0, 100.0),
        DISKIOUTILS("diskIOUtils", 0.5, 0.0, 100.0),
        DISKIOUTILS5MIN("diskIOUtils5min", 0.5, 0.0, 100.0),
        DISKSPACEAVAILABLE("diskSpaceAvailable", 0.5, 0.0, 100.0),
        DISKINODEAVAILABLE("diskInodeAvailable", 0.5, 0.0, 100.0);

        public static List<WorkerMertric> getShortTimeWorkerMertric() {
            return  Arrays.asList(CPULOADAVERAGE, CPUAVAILABLE, NETWORKINBYTES, NETWORKOUTBYTES, ALIVECONNECTION,
                    DISKREAD, DISKWRITE, DISKIOUTILS, DISKSPACEAVAILABLE, DISKINODEAVAILABLE);
        }

        public static List<WorkerMertric> getWorkerMertric() {
            return  Arrays.asList(CPULOADAVERAGE, CPUAVAILABLE, NETWORKINBYTES5MIN, NETWORKOUTBYTES5MIN, ALIVECONNECTION,
                    DISKREAD5MIN, DISKWRITE5MIN, DISKIOUTILS5MIN, DISKSPACEAVAILABLE, DISKINODEAVAILABLE);
        }

        private String name ;
        private Double weight ;
        private Double max ;
        private Double min ;

        private WorkerMertric(String name , Double weight, Double min, Double max) {
            this.name = name;
            this.weight = weight;
            this.min = min;
            this.max = max;
        }
    }

    public double maxMinNormalization(double Max, double Min, double x) {
        return  x = 1.0 * (x - Min) / (Max - Min);
    }

    public double computeSigama(double[] weights, double[] values) {
       double score = 0;
       for (int i = 0; i < weights.length; i++)
           score += weights[i] * values[i] ;
        return score;
    }

    public double computeWorkerScore(WorkerPressure pressure, boolean shortTime) {
        List<Double> metrics = new ArrayList();
        long[][] diskInfos = pressure.diskInfo;
        if (shortTime) {
            metrics.add(maxMinNormalization(WorkerMertric.CPULOADAVERAGE.max, WorkerMertric.CPULOADAVERAGE.min, pressure.cpuLoadAverage));
            metrics.add(maxMinNormalization(WorkerMertric.CPUAVAILABLE.max, WorkerMertric.CPUAVAILABLE.min, pressure.cpuAvailable));
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKINBYTES.max, WorkerMertric.NETWORKINBYTES.min, pressure.networkInBytes));
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKOUTBYTES.max, WorkerMertric.NETWORKOUTBYTES.min, pressure.networkOutBytes));
            metrics.add(maxMinNormalization(WorkerMertric.ALIVECONNECTION.max, WorkerMertric.ALIVECONNECTION.min, pressure.aliveConnection));
            for(int i = 0; i < diskInfos.length; i++) {
                metrics.add(maxMinNormalization(WorkerMertric.DISKREAD.max, WorkerMertric.DISKREAD.min, diskInfos[i][0]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKWRITE.max, WorkerMertric.DISKWRITE.min, diskInfos[i][2]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKIOUTILS.max, WorkerMertric.DISKIOUTILS.min, diskInfos[i][4]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKSPACEAVAILABLE.max, WorkerMertric.DISKSPACEAVAILABLE.min, diskInfos[i][6]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKINODEAVAILABLE.max, WorkerMertric.DISKINODEAVAILABLE.min, diskInfos[i][7]));
            }
        } else {
            metrics.add(maxMinNormalization(WorkerMertric.CPULOADAVERAGE.max, WorkerMertric.CPULOADAVERAGE.min, pressure.cpuLoadAverage));
            metrics.add(maxMinNormalization(WorkerMertric.CPUAVAILABLE.max, WorkerMertric.CPUAVAILABLE.min, pressure.cpuAvailable));
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKINBYTES5MIN.max, WorkerMertric.NETWORKINBYTES5MIN.min, pressure.networkInBytes5min));
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKOUTBYTES5MIN.max, WorkerMertric.NETWORKOUTBYTES5MIN.min, pressure.networkOutBytes5min));
            metrics.add(maxMinNormalization(WorkerMertric.ALIVECONNECTION.max, WorkerMertric.ALIVECONNECTION.min, pressure.aliveConnection));
            for(int i = 0; i < diskInfos.length; i++) {
                metrics.add(maxMinNormalization(WorkerMertric.DISKREAD5MIN.max, WorkerMertric.DISKREAD5MIN.min, diskInfos[i][1]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKWRITE5MIN.max, WorkerMertric.DISKWRITE5MIN.min, diskInfos[i][3]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKIOUTILS5MIN.max, WorkerMertric.DISKIOUTILS5MIN.min, diskInfos[i][5]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKSPACEAVAILABLE.max, WorkerMertric.DISKSPACEAVAILABLE.min, diskInfos[i][6]));
                metrics.add(maxMinNormalization(WorkerMertric.DISKINODEAVAILABLE.max, WorkerMertric.DISKINODEAVAILABLE.min, diskInfos[i][7]));
            }
        }
        Double[] values = metrics.stream().toArray(Double[]::new);
        double[] weights;
        if (shortTime) {
            weights = WorkerMertric.getShortTimeWorkerMertric().stream().mapToDouble(workerMertric -> workerMertric.weight).toArray();
        } else {
            weights = WorkerMertric.getWorkerMertric().stream().mapToDouble(workerMertric -> workerMertric.weight).toArray();
        }
        return computeSigama(weights, ArrayUtils.toPrimitive(values));
    }

    public boolean filterBadGagueWorker(WorkerInfo worker) {
        WorkerPressure pressure = worker.getPressure();
        long[][] diskInfos = pressure.diskInfo;
        double half = Math.ceil(1.0 * diskInfos.length / 2);
        int ioUtilCount = 0;
        int diskSpaceCount = 0;
        int diskInodeCount = 0;
        for (int i = 0; i < diskInfos.length; i++) {
            if (1.0 * diskInfos[i][4] / 100 > diskIoUtilThreshold) ioUtilCount++ ;
            if (1.0 * diskInfos[i][6] / 100 < diskSpaceThreshold) diskSpaceCount++ ;
            if (1.0 * diskInfos[i][7] / 100 < diskInodeThreshold) diskInodeCount++ ;
        }
        if (ioUtilCount > half || diskSpaceCount > half || diskInodeCount > half){
            return true;
        } else {
            return false;
        }
    }

    public void updateBusyAndBlackWorkers(WorkerInfo worker) {
        double score = worker.getScore();
        LinkedList<Double> historyScores = worker.historyScores;
        if (score > blackScore) {
            blackWorkers.add(worker);
        } else if (score > busyScore) {
            busyWorkers.add(worker);
        } else {
         long count = historyScores.stream().filter(s -> s.doubleValue() > busyScore).count();
         if (count == wellTime) {
             blackWorkers.remove(worker);
             busyWorkers.remove(worker);
         }
        }
    }

}



