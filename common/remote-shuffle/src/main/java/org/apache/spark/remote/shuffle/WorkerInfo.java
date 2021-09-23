package org.apache.spark.remote.shuffle;

import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.remote.shuffle.protocol.CleanApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;


public class WorkerInfo {
    private static final Logger logger = LoggerFactory.getLogger(WorkerInfo.class);

    private String host;
    private int port;
    private long latestHeartbeatTime;
    private WorkerPressure pressure;

    private TransportClientFactory clientFactory;



    public WorkerInfo(TransportClientFactory clientFactory, String host, int port) {
        this.host = host;
        this.port = port;
        this.clientFactory = clientFactory;
    }

    public void cleanApplication(String applicationId, int attemptId) {
        try {
            TransportClient client = clientFactory.createClient(host, port);
            CleanApplication application = new CleanApplication(applicationId, attemptId);
            client.sendRpc(application.toByteBuffer(), new CleanApplicationCallback(application));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public long getLatestHeartbeatTime() {
        return latestHeartbeatTime;
    }

    public void setLatestHeartbeatTime(long latestHeartbeatTime) {
        this.latestHeartbeatTime = latestHeartbeatTime;
    }

    public WorkerPressure getPressure() {
        return pressure;
    }

    public void setPressure(WorkerPressure pressure) {
        this.pressure = pressure;
    }


    private class CleanApplicationCallback implements RpcResponseCallback {
        private CleanApplication application;

        public CleanApplicationCallback(CleanApplication application) {
            this.application = application;
        }

        @Override
        public void onFailure(Throwable e) {
            logger.warn("Host {} clean application {} failure", host + ":" + port, application.getAppId() + "_" + application.getAttempt());
        }

        @Override
        public void onSuccess(ByteBuffer response) {
            logger.info("Host {} clean application {} success", host + ":" + port, application.getAppId() + "_" + application.getAttempt());
        }
    }

}
