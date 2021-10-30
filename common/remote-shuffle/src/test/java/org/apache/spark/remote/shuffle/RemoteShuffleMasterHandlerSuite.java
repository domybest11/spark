package org.apache.spark.remote.shuffle;/*
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

import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.Assert;
import org.junit.Test;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class RemoteShuffleMasterHandlerSuite {

    @Test
    public void testMasterRecovery() throws Exception {

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


}
