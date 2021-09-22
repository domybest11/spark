package org.apache.spark.remote.shuffle;

import org.apache.spark.network.client.TransportClient;

public class WorkerInfo {
    private String host;
    private int port;
    private long latestHeartbeatTime;
    private WorkerPressure pressure;

    public WorkerInfo(String host, int port) {
        this.host = host;
        this.port = port;
        latestHeartbeatTime = System.currentTimeMillis();
    }

    public void cleanApplication(String applicationId, int attemptId){

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
}
