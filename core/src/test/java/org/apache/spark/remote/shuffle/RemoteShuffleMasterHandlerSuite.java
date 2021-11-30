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

import com.google.common.collect.Lists;
import org.apache.spark.deploy.RemoteShuffleMasterHandler;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.remote.RemoteShuffleWorkerHeartbeat;
import org.apache.spark.network.shuffle.protocol.remote.RunningStage;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;


public class RemoteShuffleMasterHandlerSuite {

    @Test
    public void testMasterRecovery() throws Exception {
        long[] lon = new long[2];
        RunningStage[] runningStages = new RunningStage[0];
        List<RunningStage> currentRunningStages = new ArrayList<>();
        RemoteShuffleWorkerHeartbeat workerHeartbeat =
                new RemoteShuffleWorkerHeartbeat(
                        "local",
                        0,
                        System.currentTimeMillis(),
                        lon,
                        currentRunningStages.toArray(new RunningStage[0])
                );
        ByteBuffer byteBuffer = workerHeartbeat.toByteBuffer();
        Map<String, String> config = new HashMap<>();
        config.put("spark.shuffle.master.recovery.path", "/Users/jiadongdong/Downloads/testDB");
        MapConfigProvider map = new MapConfigProvider(config);
        TransportConf conf = new TransportConf("shuffle", map);
        RemoteShuffleMasterHandler rsmh = new RemoteShuffleMasterHandler("localhost", 0, conf);
        rsmh.runningApplicationMap.computeIfAbsent("app01", f -> {
            RemoteShuffleMasterHandler.RunningApplication runApp =  new RemoteShuffleMasterHandler.RunningApplication("app01", 0);
            TransportContext transportContext = new TransportContext(conf, null, true);
            TransportClientFactory clientFactory = transportContext.createClientFactory(Lists.newArrayList());
            Set<WorkerInfo> workerInfos = new HashSet<>();
            WorkerInfo workerInfo = new WorkerInfo(clientFactory, "localhost", 0);
            workerInfos.add(workerInfo);
            runApp.setWorkerInfos(workerInfos);
            rsmh.saveMasterStateDB("app01", runApp, rsmh.RUNNING_APP_PREFIX);
            return runApp;
        });
        rsmh.runningApplicationMap.computeIfAbsent("app02", f -> {
            RemoteShuffleMasterHandler.RunningApplication runApp =  new RemoteShuffleMasterHandler.RunningApplication("app02", 0);
            rsmh.saveMasterStateDB("app02", runApp, rsmh.RUNNING_APP_PREFIX);
            return runApp;
        });
        TransportContext transportContext = new TransportContext(conf, null, true);
        TransportClientFactory clientFactory = transportContext.createClientFactory(Lists.newArrayList());
        WorkerInfo workerInfo01 = new WorkerInfo(clientFactory, "localhost01", 0);
        WorkerInfo workerInfo02 = new WorkerInfo(clientFactory, "localhost02", 1);
        rsmh.blackWorkers.add(workerInfo01);
        rsmh.saveMasterStateDB("localhost01", workerInfo01, rsmh.BLACK_WORKERS_PREFIX);
        rsmh.blackWorkers.add(workerInfo02);
        rsmh.saveMasterStateDB("localhost02", workerInfo02, rsmh.BLACK_WORKERS_PREFIX);
        workerInfo02.setHost("localhost03");
        rsmh.saveMasterStateDB("localhost02", workerInfo02, rsmh.BLACK_WORKERS_PREFIX);
        rsmh.db.close();
        RemoteShuffleMasterHandler rsmh1 = new RemoteShuffleMasterHandler("localhost", 0, conf);
        Assert.assertEquals(2, rsmh1.runningApplicationMap.size());
        Assert.assertEquals(2, rsmh1.blackWorkers.size());
    }

    @Test
    public void testWorkerPressure() throws Exception {
        Long[] lon = new Long[]{1L, 2L, 3L};
        List<Long> diskInfos = new ArrayList<Long>(Arrays.asList(lon));
        Iterator iterator = diskInfos.iterator();
        while(iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        System.out.println(diskInfos.size());

        long[] metrics = new long[]{96, 48, 3851, 2300, 3971, 2109, 0,
                0, 42, 280, 138, 26, 18, 15, 1, 0,
                0, 0, 263, 113, 29, 15, 12, 1, 0,
                0, 0, 234, 119, 40, 20, 1, 1, 0,
                0, 0, 218, 185, 37, 28, 1, 1, 1,
                0, 0, 350, 137, 62, 23, 1, 1, 1,
                0, 0, 281, 136, 48, 24, 1, 1, 1,
                0, 0, 140, 115, 22, 20, 1, 1, 1,
                0, 0, 150, 110, 26, 18, 1, 1, 1,
                0, 0, 173, 105, 32, 20, 1, 1, 1,
                0, 0, 240, 109, 41, 19, 1, 1, 1,
                0, 0, 212, 123, 38, 21, 1, 1, 1,
                0, 0, 176, 106, 39, 18, 1, 1, 1,
                0, 0, 250, 112, 40, 19, 1, 1, 1,
                0, 0, 436, 142, 67, 22, 1, 1, 1,
                14};
        WorkerPressure pressure = new WorkerPressure(metrics);
        Map<String, String> config = new HashMap<>();
        MapConfigProvider map = new MapConfigProvider(config);
        TransportConf conf = new TransportConf("shuffle", map);
        RemoteShuffleMasterHandler handler = new RemoteShuffleMasterHandler("", 0, conf);
        double score = handler.computeWorkerScore(pressure, false);
        Assert.assertEquals(0.16, score);
    }


    @Test
    public void testGetMergerWorkers() throws Exception {
        Map<String, String> config = new HashMap<>();
        MapConfigProvider map = new MapConfigProvider(config);
        TransportConf conf = new TransportConf("shuffle", map);
        RemoteShuffleMasterHandler handler = new RemoteShuffleMasterHandler("", 0, conf);
        LinkedList<Double> historyScores = new LinkedList<Double>();
        for (int i = 0 ; i < 100; i++) {
            if (historyScores.size() >= 5) {
                historyScores.removeFirst();
            }
            historyScores.add(1.0 * i);
        }
        Assert.assertEquals(5, historyScores.size());
        TransportContext transportContext = new TransportContext(conf, null, true);
        TransportClientFactory clientFactory = transportContext.createClientFactory(Lists.newArrayList());
        for (int i = 0 ; i < 300; i++) {

            long[] metrics = new long[]{96, 48, 3851, 2300, 3971, 2109, 0,
                0, 42, 280, 138, 26, 18, 15, 1,
                0, 0, 263, 113, 29, 15, 12, 1,
                0, 0, 234, 119, 40, 20, 1, 1,
                0, 0, 218, 185, 37, 28, 1, 1,
                0, 0, 350, 137, 62, 23, 1, 1,
                0, 0, 281, 136, 48, 24, 1, 1,
                0, 0, 140, 115, 22, 20, 1, 1,
                0, 0, 150, 110, 26, 18, 1, 1,
                0, 0, 173, 105, 32, 20, 1, 1,
                0, 0, 240, 109, 41, 19, 1, 1,
                0, 0, 212, 123, 38, 21, 1, 1,
                0, 0, 176, 106, 39, 18, 1, 1,
                0, 0, 250, 112, 40, 19, 1, 1,
                0, 0, 436, 142, 67, 22, 1, 1,
                14};
            WorkerPressure pressure = new WorkerPressure(metrics);
            WorkerInfo w = new WorkerInfo(clientFactory, i+"", i);
            w.setPressure(pressure);
            handler.workersMap.put(i + ":" + i, w);
            if (i % 2 == 0) {
                handler.busyWorkers.add(w);
            }
            if (i % 25 == 0 && i % 2 != 0) {
                handler.blackWorkers.add(w);
            }
        }
        int count = 0;
        for (int i =0; i< handler.workersMap.size(); i++) {
            if (i % 25 != 0 && i % 2 != 0 && count < 10) {
                long[] metrics = new long[]{96, 48, 3851, 2300, 3971, 2109, 0,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    1400, 1400, 1400, 1400, 99, 99, 99, 99,
                    14};
                WorkerPressure pressure = new WorkerPressure(metrics);
                WorkerInfo w = new WorkerInfo(clientFactory, i+"", i);
                w.setPressure(pressure);
                handler.workersMap.put(i + ":" + i, w);
                count++;
            }
        }

        WorkerInfo w = handler.workersMap.get("0:0");
        w.setScore(0.8);
        w.historyScores.add(0.8);
        handler.updateBusyAndBlackWorkers(w);
        WorkerInfo w1 = handler.workersMap.get("0:0");
        w1.setScore(0.81);
        w1.historyScores.add(0.81);
        WorkerInfo w2 = handler.workersMap.get("0:0");
        w2.setScore(0.82);
        w2.historyScores.add(0.82);
        handler.updateBusyAndBlackWorkers(w2);
        WorkerInfo w3 = handler.workersMap.get("0:0");
        w3.setScore(0.83);
        w3.historyScores.add(0.83);
        handler.updateBusyAndBlackWorkers(w3);
        WorkerInfo w4 = handler.workersMap.get("0:0");
        w4.setScore(0.84);
        w4.historyScores.add(0.84);
        handler.updateBusyAndBlackWorkers(w4);
        WorkerInfo w5 = handler.workersMap.get("0:0");
        w5.setScore(0.85);
        w5.historyScores.add(0.85);
        handler.updateBusyAndBlackWorkers(w5);

        handler.getMergerWorkers("1", 0, 0, 500, 4, 800);



    }



}
