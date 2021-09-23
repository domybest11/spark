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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.*;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.remote.shuffle.protocol.CleanApplication;
import org.apache.spark.remote.shuffle.protocol.RegisterWorker;
import org.apache.spark.remote.shuffle.protocol.RemoteShuffleServiceHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// remote worker
public class RemoteBlockHandler extends ExternalBlockHandler {
    private static final Logger logger = LoggerFactory.getLogger(RemoteBlockHandler.class);
    private volatile TransportClientFactory clientFactory;
    private TransportConf transportConf;
    private long heartbeatIntervalMs = 60 * 1000L;
    private long monitorIntervalMs = 60 * 1000L;


    //meta
    private ConcurrentHashMap<String, ShuffleDir> workDirs = new ConcurrentHashMap<>();
    // key: appid_attempt  value: runningStage
    private ConcurrentHashMap<String, List<RunningStage>> appStageMap = new ConcurrentHashMap<>();
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


    public RemoteBlockHandler(TransportConf conf, File registeredExecutorFile) throws IOException {
        super(conf, registeredExecutorFile);
        transportConf = conf;
        init(conf);
    }


    private void init(TransportConf conf) throws IOException {
        TransportContext context = new TransportContext(
                transportConf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        clientFactory = context.createClientFactory(bootstraps);
        try {
            String masterHost = "";
            int masterPort = 0;
            String dirsConfig = conf.get("shuffle.worker.dirs", "");
            if (StringUtils.isBlank(dirsConfig)) {
                throw new IOException("");
            } else {
                Arrays.stream(dirsConfig.split(",")).forEach(dir -> {
                    if (dir.contains(":")) {
                        String[] arr = dir.split(":");
                        workDirs.put(dir, new ShuffleDir(arr[0], DiskType.toDiskType(arr[1])));
                    } else {
                        workDirs.put(dir, new ShuffleDir(dir, DiskType.HDD));
                    }
                });
                registerRemoteShuffleWorker(masterHost, masterPort);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void registerRemoteShuffleWorker(
            String host,
            int port) throws IOException, InterruptedException {
        TransportClient client = clientFactory.createClient(host, port);
        ByteBuffer registerWorker = new RegisterWorker("", 0).toByteBuffer();
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
            // TODO: 2021/9/23 如果是第一次push构建这个app 的数据结构
            String key = message.appId + "_" + message.appAttemptId;
            appStageMap.computeIfAbsent(key, v -> {
                return null;
            });
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
            String key = cleanApplication.getAppId() + "_" + cleanApplication.getAttempt();
            List<RunningStage> stages = appStageMap.remove(key);
        } else {
            super.handleMessage(msgObj, client, callback);
        }
    }

    @Override
    public void close() {
        super.close();
        heartbeatThread.shutdownNow();
    }


    private class RegisterWorkerCallback implements RpcResponseCallback {
        private final TransportClient client;

        public RegisterWorkerCallback(TransportClient client) {
            this.client = client;
        }

        @Override
        public void onFailure(Throwable e) {
            logger.warn("Unable to registered remote shuffle worker");
        }

        @Override
        public void onSuccess(ByteBuffer response) {
            heartbeatThread.scheduleAtFixedRate(
                    new Heartbeat(client), 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);

            pressureMonitorThread.scheduleAtFixedRate(
                    new PressureMonitor(), 0, monitorIntervalMs, TimeUnit.MILLISECONDS);

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
            appStageMap.values().forEach(currentRunningStages::addAll);
            // TODO: 2021/9/22 pressure相关
            client.send(
                    new RemoteShuffleServiceHeartbeat(
                            "host",
                            0,
                            System.currentTimeMillis(),
                            null,
                            currentRunningStages.toArray(new RunningStage[0])).toByteBuffer()
            );
        }
    }

    private class PressureMonitor implements Runnable {

        private PressureMonitor() {
        }

        @Override
        public void run() {
        }
    }

}
