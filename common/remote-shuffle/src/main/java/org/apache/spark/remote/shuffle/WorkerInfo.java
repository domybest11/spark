package org.apache.spark.remote.shuffle;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.remote.CleanApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Objects;


public class WorkerInfo {

    private static final Logger logger = LoggerFactory.getLogger(WorkerInfo.class);
    private String host;
    private int port;
    private long latestHeartbeatTime;
    private WorkerPressure pressure;
    private long startTime;
    public LinkedList<Double> historyScores = new LinkedList<Double>();
    private Double score = 0.0;

    private TransportClientFactory clientFactory;

    public WorkerInfo() {
    }

    public WorkerInfo(TransportClientFactory clientFactory, String host, int port) {
        this.host = host;
        this.port = port;
        this.clientFactory = clientFactory;
        this.latestHeartbeatTime = System.currentTimeMillis();
        this.startTime = System.currentTimeMillis();
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Double getScore() {
        return score;
    }

    public boolean isShortTime(long time) {
       return startTime - time < 5_60_1000L;
    }

    public void setClientFactory(TransportClientFactory clientFactory) {
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

    public void setHost(String host) {
        this.host = host;
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

    // TODO: 2021/11/1
    public void setPressure(WorkerPressure pressure) {
        this.pressure = pressure;
    }

    public String address() {
        return host+ ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerInfo that = (WorkerInfo) o;
        return port == that.port && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
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
