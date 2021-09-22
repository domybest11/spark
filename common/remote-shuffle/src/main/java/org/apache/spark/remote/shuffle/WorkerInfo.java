package org.apache.spark.remote.shuffle;

import org.apache.spark.network.client.TransportClient;

public class WorkerInfo {
    private String host;
    private int port;
    private long latestHeartbeatTime;

    public WorkerInfo(String host, int port) {
        this.host = host;
        this.port = port;
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
}
