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
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.shuffle.protocol.AbstractFetchShuffleBlocks;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.remote.shuffle.protocol.RegisterWorker;
import org.apache.spark.remote.shuffle.protocol.RemoteShuffleServiceHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
    private ConcurrentHashMap<ShuffleDir,List<RunningStage>> dirInfo;



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
        init();
    }


    private void init() {
        TransportContext context = new TransportContext(
                transportConf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        clientFactory = context.createClientFactory(bootstraps);
        try {
            String masterHost = "";
            int masterPort = 0;
            registerRemoteShuffleWorker(masterHost,masterPort);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void registerRemoteShuffleWorker(
            String host,
            int port) throws IOException, InterruptedException {
        TransportClient client = clientFactory.createClient(host, port);
        ByteBuffer registerWorker = new RegisterWorker("",0).toByteBuffer();
        client.sendRpc(registerWorker, new RegisterWorkerCallback(client));
    }


    @Override
    protected void handleMessage(BlockTransferMessage msgObj, TransportClient client, RpcResponseCallback callback) {
        if (msgObj instanceof AbstractFetchShuffleBlocks){

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
            client.send(new RemoteShuffleServiceHeartbeat("host", 0,  1L, null, null).toByteBuffer());
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
