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
import org.apache.spark.remote.shuffle.protocol.CleanApplication;
import org.apache.spark.remote.shuffle.protocol.RegisterWorker;
import org.apache.spark.remote.shuffle.protocol.RemoteShuffleServiceHeartbeat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class RemoteMaster {
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
    public final ArrayList<WorkerInfo> workers = new ArrayList<>();
    public final ArrayList<WorkerInfo> blackWorkers = new ArrayList<>();
    public final ArrayList<WorkerInfo> busyWorkers = new ArrayList<>();




    private void start() {
        if (server == null) {
            transportContext = new TransportContext(transportConf, new MasterRpcHandler(), true);
            List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
            server = transportContext.createServer(port, bootstraps);
            clientFactory = transportContext.createClientFactory(Lists.newArrayList());
        }
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

        private ApplicationExpire() {
        }

        @Override
        public void run() {
            try {
                TransportClient client = clientFactory.createClient("host", port);
                client.send(new CleanApplication().toByteBuffer());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static class MasterRpcHandler extends RpcHandler {

        @Override
        public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
            BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
            handleMessage(msgObj, client, callback);
        }

        private void handleMessage(BlockTransferMessage msgObj, TransportClient client, RpcResponseCallback callback) {
            if (msgObj instanceof RegisterWorker) {

            } else if (msgObj instanceof RemoteShuffleServiceHeartbeat) {

            } else {
                throw new UnsupportedOperationException("Unexpected message: " + msgObj);
            }
        }

        @Override
        public StreamManager getStreamManager() {
            return null;
        }

    }
}



