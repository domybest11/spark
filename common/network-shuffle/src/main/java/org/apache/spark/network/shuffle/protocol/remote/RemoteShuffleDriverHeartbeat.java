package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RemoteShuffleDriverHeartbeat  extends BlockTransferMessage {
    private String appId;
    private int attempt;
    private final long heartbeatTimeoutMs;

    public RemoteShuffleDriverHeartbeat(String appId, int attempt, long heartbeatTimeoutMs) {
        this.appId = appId;
        this.attempt = attempt;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public String getAppId() {
        return appId;
    }

    public int getAttempt() {
        return attempt;
    }

    public long getHeartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    @Override
    public int encodedLength() {
        return 0;
    }

    @Override
    public void encode(ByteBuf buf) {

    }

    @Override
    protected Type type() {
        return null;
    }
}
