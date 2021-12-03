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
package org.apache.spark.deploy;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.metrics.MetricsSystemInstances;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.*;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.remote.*;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LevelDBProvider;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.remote.shuffle.WorkerInfo;
import org.apache.spark.remote.shuffle.WorkerPressure;
import org.apache.spark.util.Utils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
    private int workCheckPeriod;
    private int appCheckPeriod;
    private Double diskIoUtilThreshold;
    private Double diskSpaceThreshold;
    private Double diskInodeThreshold;
    protected MetricsSystem masterMetricsSystem;
    private ExternalShuffleServiceSource masterServiceSource;
    private ExternalBlockHandler.ShuffleMetrics metrics;

    public DB db;
    private static final LevelDBProvider.StoreVersion CURRENT_VERSION = new LevelDBProvider.StoreVersion(1, 0);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final long applicationExpireTimeout;
    private final long workerExpireTimeout;

    private final ScheduledExecutorService cleanThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("remote-shuffle-master-clean")
                            .build());

    private final ScheduledExecutorService workerCheckThread =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("remote-shuffle-worker-check")
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
        this.workerExpireTimeout = JavaUtils.timeStringAsSec(conf.get("spark.shuffle.remote.worker.expire","300s")) * 1000;
        this.busyScore = Double.parseDouble(conf.get("spark.shuffle.worker.busyScore","0.9"));
        this.blackScore = Double.parseDouble(conf.get("spark.shuffle.worker.blackScore","0.95"));
        this.wellTime = Integer.parseInt(conf.get("spark.shuffle.worker.wellTime","5"));
        this.workCheckPeriod = Integer.parseInt(conf.get("spark.shuffle.worker.checkPeriod","60"));
        this.appCheckPeriod = Integer.parseInt(conf.get("spark.shuffle.app.checkPeriod","60"));
        this.diskIoUtilThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskIOUtils","0.9"));
        this.diskSpaceThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskSpaceUsed","0.9"));
        this.diskInodeThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskInodeUsed","0.9"));
        String filePath = conf.get("spark.shuffle.master.recovery.path", null);
        metrics = new ExternalBlockHandler.ShuffleMetrics();
        SparkConf sparkConf = new SparkConf();
        Utils.loadDefaultSparkProperties(sparkConf, null);
        org.apache.spark.SecurityManager securityManager =
            new org.apache.spark.SecurityManager(sparkConf, null, null);
        masterMetricsSystem = MetricsSystem.createMetricsSystem(MetricsSystemInstances.SHUFFLE_SERVICE(),
            sparkConf, securityManager);
        masterServiceSource = new ExternalShuffleServiceSource();
        masterServiceSource.registerMetricSet(metrics);
        masterMetricsSystem.registerSource(masterServiceSource);
        masterMetricsSystem.start(true);


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
        cleanThread.scheduleAtFixedRate(new ApplicationExpire(), 1, appCheckPeriod, TimeUnit.SECONDS);
        workerCheckThread.scheduleAtFixedRate(new WorkerExpire(), 1, workCheckPeriod, TimeUnit.SECONDS);
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
                if (System.currentTimeMillis() - runningApplication.latestHeartbeatTime > applicationExpireTimeout) {
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

    private class WorkerExpire implements Runnable {

        @Override
        public void run() {
            Iterator<WorkerInfo> it = workersMap.values().iterator();
            while (it.hasNext()){
                WorkerInfo worker = it.next();
                if (System.currentTimeMillis() - worker.latestHeartbeatTime > workerExpireTimeout) {
                    it.remove();
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
                final Timer.Context responseDelayContext =
                    metrics.registerWorkerRequestLatencyMillis.time();
                try {
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
                    logger.info("handle register worker from ip: {}, time: {}ms", address, System.currentTimeMillis() - start);
                   } finally {
                    responseDelayContext.stop();
                   }
            } else if (msgObj instanceof RemoteShuffleWorkerHeartbeat) {
                final Timer.Context responseDelayContext =
                metrics.workerHeartbeatRequestLatencyMillis.time();
                try {
                    long start = System.currentTimeMillis();
                    RemoteShuffleWorkerHeartbeat workHeartbeat = (RemoteShuffleWorkerHeartbeat) msgObj;
                    String host = workHeartbeat.getHost();
                    int port = workHeartbeat.getPort();
                    String address = host + ":" + port;
                    logger.info("handle RemoteShuffleServiceHeartbeat from ip: {}", address);
                    RunningStage[] runningStages = workHeartbeat.getRunningStages();
                    WorkerPressure pressure = new WorkerPressure(workHeartbeat.getWorkerMetric());
                    WorkerInfo workerInfo = workersMap.compute(address, (id, w) -> {
                        if (w != null) {
                            double score = computeWorkerScore(pressure, w.isShortTime(System.currentTimeMillis()));
                            logger.info("worker from ip: {}, pressure: {}", address, workHeartbeat.getWorkerMetric());
                            logger.info("worker from ip: {}, score: {}", address, score);
                            LinkedList<Double> historyScores = w.historyScores;
                            int size = historyScores.size();
                            if (size >= wellTime) {
                                historyScores.removeFirst();
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
                            runningApplication.setLatestHeartbeatTime(System.currentTimeMillis());
                            runningApplication.getWorkerInfos().add(workerInfo);
                        }
                    });
                    logger.info("handle RemoteShuffleServiceHeartbeat time: {}ms", System.currentTimeMillis() - start);
                } finally {
                    responseDelayContext.stop();
                }
            } else if (msgObj instanceof UnregisterWorker) {
                final Timer.Context responseDelayContext =
                    metrics.unregisterWorkerRequestLatencyMillis.time();
                try {
                    long start = System.currentTimeMillis();
                    UnregisterWorker unregisterWorker = (UnregisterWorker) msgObj;
                    String host = unregisterWorker.getHost();
                    int port = unregisterWorker.getPort();
                    String address = host + ":" + port;
                    deleteMasterStateDB(address, WORKERS_PREFIX);
                    WorkerInfo workerInfo = workersMap.remove(address);
                    blackWorkers.remove(workerInfo);
                    busyWorkers.remove(workerInfo);
                    logger.info("handle unregisterWorker time: {}ms", System.currentTimeMillis() - start);
                } finally {
                responseDelayContext.stop();
                }
            } else if (msgObj instanceof RegisterApplication) {
                final Timer.Context responseDelayContext =
                    metrics.registerApplicationRequestLatencyMillis.time();
                try {
                    RegisterApplication application = (RegisterApplication) msgObj;
                    String appId = application.getAppId();
                    int attemptId = application.getAttempt();
                    String key = application.getKey();
                    RunningApplication runningApplication = runningApplicationMap.computeIfAbsent(key, f -> new RunningApplication(appId, attemptId));
                    callback.onSuccess(ByteBuffer.wrap(new byte[0]));
                    logger.info("application: {}_{} register success", runningApplication.appId, runningApplication.attemptId);
                } finally {
                    responseDelayContext.stop();
                }
            } else if (msgObj instanceof UnregisterApplication) {
                final Timer.Context responseDelayContext =
                    metrics.unregisterApplicationRequestLatencyMillis.time();
                try {
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
                } finally {
                    responseDelayContext.stop();
                }
            } else if (msgObj instanceof RemoteShuffleDriverHeartbeat) {
                final Timer.Context responseDelayContext =
                    metrics.driverHeartbeatRequestLatencyMillis.time();
                try {
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
                } finally {
                    responseDelayContext.stop();
                }
            }  else if (msgObj instanceof GetPushMergerLocations) {
                final Timer.Context responseDelayContext =
                    metrics.getPushMergerLocationsRequestLatencyMillis.time();
                try {
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
                } finally {
                    responseDelayContext.stop();
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

        CPULOADAVERAGE("cpuLoadAverage", 0.1, 0.0, 200.0),
        CPUAVAILABLE("cpuAvailable", 0.1, 0.0, 200.0),
        NETWORKINBYTES("networkInBytes", 0.1, 0.0, 10240.0),
        NETWORKINBYTES5MIN("networkInBytes5min", 0.1, 0.0, 10240.0),
        NETWORKOUTBYTES("networkOutBytes", 0.1, 0.0, 10240.0),
        NETWORKOUTBYTES5MIN("networkOutBytes5min", 0.1, 0.0, 10240.0),
        ALIVECONNECTION("aliveConnection", 0.1, 0.0, 10000.0),
        //HDD
        HDDDISKREAD("diskReadIOps", 0.1, 0.0, 300.0),
        HDDDISKREAD5MIN("diskReadIOps5min", 0.1, 0.0, 300.0),
        HDDDISKWRITE("diskWriteIOps", 0.1, 0.0, 300.0),
        HDDDISKWRITE5MIN("diskWriteIOps5min", 0.1, 0.0, 300.0),
        //SSD
        SSDDISKREAD("diskReadIOps", 0.1, 0.0, 2500.0),
        SSDDISKREAD5MIN("diskReadIOps5min", 0.1, 0.0, 2500.0),
        SSDDISKWRITE("diskWriteIOps", 0.1, 0.0, 2500.0),
        SSDDISKWRITE5MIN("diskWriteIOps5min", 0.1, 0.0, 2500.0),

        DISKIOUTILS("diskIOUtils", 0.1, 0.0, 100.0),
        DISKIOUTILS5MIN("diskIOUtils5min", 0.1, 0.0, 100.0),
        DISKSPACEUSED("diskSpaceUsed", 0.1, 0.0, 100.0),
        DISKINODEAUSED("diskInodeUsed", 0.1, 0.0, 100.0);

        public static List<WorkerMertric> getShortTimeWorkerMertric() {
            return  Arrays.asList(CPULOADAVERAGE, CPUAVAILABLE, NETWORKINBYTES, NETWORKOUTBYTES, ALIVECONNECTION,
                    HDDDISKREAD, HDDDISKWRITE, DISKIOUTILS, DISKSPACEUSED, DISKINODEAUSED);
        }

        public static List<WorkerMertric> getWorkerMertric() {
            return  Arrays.asList(CPULOADAVERAGE, CPUAVAILABLE, NETWORKINBYTES5MIN, NETWORKOUTBYTES5MIN, ALIVECONNECTION,
                    HDDDISKREAD5MIN, HDDDISKWRITE5MIN, DISKIOUTILS5MIN, DISKSPACEUSED, DISKINODEAUSED);
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
        BigDecimal bd = new BigDecimal(score).setScale(2, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public double computeWorkerScore(WorkerPressure pressure, boolean shortTime) {
        List<Double> metrics = new ArrayList();
        long[][] diskInfos = pressure.diskInfo;
        int ssdCount = 0;
        int hddCount = 0;
        double hddDiskReadIOps = 0.0;
        double hddDiskWriteIOps = 0.0;
        double ssdDiskReadIOps = 0.0;
        double ssdDiskWriteIOps = 0.0;
        double diskIOUtils = 0.0;
        double diskSpaceUsed = 0.0;
        double diskInodeUsed = 0.0;
        metrics.add(maxMinNormalization(WorkerMertric.CPULOADAVERAGE.max, WorkerMertric.CPULOADAVERAGE.min, pressure.cpuLoadAverage));
        metrics.add(maxMinNormalization(WorkerMertric.CPUAVAILABLE.max, WorkerMertric.CPUAVAILABLE.min, pressure.cpuAvailable));
        metrics.add(maxMinNormalization(WorkerMertric.ALIVECONNECTION.max, WorkerMertric.ALIVECONNECTION.min, pressure.aliveConnection));
        if (shortTime) {
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKINBYTES.max, WorkerMertric.NETWORKINBYTES.min, pressure.networkInBytes));
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKOUTBYTES.max, WorkerMertric.NETWORKOUTBYTES.min, pressure.networkOutBytes));
            for(int i = 0; i < diskInfos.length; i++) {
                if (diskInfos[i][8] == 0) {
                    ssdDiskReadIOps += diskInfos[i][0];
                    ssdDiskWriteIOps += diskInfos[i][2];
                    ssdCount++;
                } else {
                   hddDiskReadIOps += diskInfos[i][0];
                   hddDiskWriteIOps += diskInfos[i][2];
                }
                diskIOUtils += diskInfos[i][4];
                diskSpaceUsed += diskInfos[i][6];
                diskInodeUsed += diskInfos[i][7];
            }
        } else {
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKINBYTES5MIN.max, WorkerMertric.NETWORKINBYTES5MIN.min, pressure.networkInBytes5min));
            metrics.add(maxMinNormalization(WorkerMertric.NETWORKOUTBYTES5MIN.max, WorkerMertric.NETWORKOUTBYTES5MIN.min, pressure.networkOutBytes5min));
            for(int i = 0; i < diskInfos.length; i++) {
                if (diskInfos[i][8] == 0) {
                    ssdDiskReadIOps += diskInfos[i][1];
                    ssdDiskWriteIOps += diskInfos[i][3];
                    ssdCount++;
                } else {
                    hddDiskReadIOps += diskInfos[i][1];
                    hddDiskWriteIOps += diskInfos[i][3];
                }
                diskIOUtils += diskInfos[i][5];
                diskSpaceUsed += diskInfos[i][6];
                diskInodeUsed += diskInfos[i][7];
            }
        }
        hddCount = diskInfos.length - ssdCount;
        double hddDiskReadMaxMin = 0;
        double ssdDiskReadMaxMin = 0;
        if (hddCount != 0) {
            hddDiskReadMaxMin =
                    maxMinNormalization(WorkerMertric.HDDDISKREAD.max, WorkerMertric.HDDDISKREAD.min, 1.0 * hddDiskReadIOps / hddCount);
        }
        if (ssdCount != 0) {
            ssdDiskReadMaxMin =
                    maxMinNormalization(WorkerMertric.SSDDISKREAD.max, WorkerMertric.SSDDISKREAD.min, 1.0 * ssdDiskReadIOps / ssdCount);
        }
        double avgDiskReadMaxMin = 1.0 * (hddDiskReadMaxMin + ssdDiskReadMaxMin) / 2;

        double hddDiskWriteMaxMin = 0;
        double ssdDiskWriteMaxMin = 0;
        if (hddCount != 0) {
            hddDiskWriteMaxMin =
                    maxMinNormalization(WorkerMertric.HDDDISKWRITE.max, WorkerMertric.HDDDISKWRITE.min, 1.0 * hddDiskWriteIOps / hddCount);
        }
        if (ssdCount != 0) {
            ssdDiskWriteMaxMin =
                    maxMinNormalization(WorkerMertric.SSDDISKWRITE.max, WorkerMertric.SSDDISKWRITE.min, 1.0 * ssdDiskWriteIOps / ssdCount);
        }
        double avgDiskWriteMaxMin = 1.0 * (hddDiskWriteMaxMin + ssdDiskWriteMaxMin) / 2;
        metrics.add(avgDiskReadMaxMin);
        metrics.add(avgDiskWriteMaxMin);
        metrics.add(maxMinNormalization(WorkerMertric.DISKIOUTILS.max, WorkerMertric.DISKIOUTILS.min, 1.0 * diskIOUtils / diskInfos.length));
        metrics.add(maxMinNormalization(WorkerMertric.DISKSPACEUSED.max, WorkerMertric.DISKSPACEUSED.min, 1.0 * diskSpaceUsed / diskInfos.length));
        metrics.add(maxMinNormalization(WorkerMertric.DISKINODEAUSED.max, WorkerMertric.DISKINODEAUSED.min, 1.0 * diskInodeUsed / diskInfos.length));
        Double[] values = metrics.stream().toArray(Double[]::new);
        double[] weights;
        if (shortTime) {
            weights = WorkerMertric.getShortTimeWorkerMertric().stream().mapToDouble(workerMertric -> workerMertric.weight).toArray();
        } else {
            weights = WorkerMertric.getWorkerMertric().stream().mapToDouble(workerMertric -> workerMertric.weight).toArray();
        }
        return computeSigama(weights, ArrayUtils.toPrimitive(values));
    }

    public List<WorkerInfo> getMergerWorkers(String appId, int attempt, int shuffleId, int numPartitions, int tasksPerExecutor, int maxExecutors) {
        int numMergersDesired = Math.min(Math.max(1, (int)Math.ceil(numPartitions * 1.0 / tasksPerExecutor)), maxExecutors);
        List<WorkerInfo> workerInfos = workersMap.values().stream()
            .filter(worker -> !busyWorkers.contains(worker))
            .filter(worker -> !blackWorkers.contains(worker))
            .collect(Collectors.toList());
        int capacity = workerInfos.size();
        if (numMergersDesired <= capacity) {
            Collections.shuffle(workerInfos);
            List<WorkerInfo> targetWorkers = workerInfos.subList(0, numMergersDesired);
            List<WorkerInfo> busyingWorkers = workersMap.values().stream()
                .filter(worker -> targetWorkers.contains(worker) && filterBadGagueWorker(worker))
                .collect(Collectors.toList());
            if (busyingWorkers.size() > 0) {
                long delta = busyingWorkers.size();
                targetWorkers.removeAll(busyingWorkers);
                Iterator<WorkerInfo> iterator = workersMap.values().iterator();
                List<WorkerInfo> resultWorkers = new ArrayList<>();
                resultWorkers.addAll(targetWorkers);
                int count = 0;
                while (iterator.hasNext() && count < delta) {
                    WorkerInfo worker = iterator.next();
                    if (!busyingWorkers.contains(worker) &&
                        !blackWorkers.contains(worker) &&
                        !busyWorkers.contains(worker) &&
                        !filterBadGagueWorker(worker)) {
                        resultWorkers.add(worker);
                        count++;
                    }
                }
                if (count < delta) {
                    targetWorkers.addAll(busyingWorkers);
                    return targetWorkers;
                } else {
                    return resultWorkers;
                }
            }
            return targetWorkers;
        } else {
            return workerInfos;
        }
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
            if (1.0 * diskInfos[i][6] / 100 > diskSpaceThreshold) diskSpaceCount++ ;
            if (1.0 * diskInfos[i][7] / 100 > diskInodeThreshold) diskInodeCount++ ;
        }
        if (ioUtilCount > half || diskSpaceCount > half || diskInodeCount > half) {
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
         long count = historyScores.stream().filter(s -> s.doubleValue() < busyScore).count();
         if (count >= wellTime) {
             blackWorkers.remove(worker);
             busyWorkers.remove(worker);
         }
        }
    }

}



