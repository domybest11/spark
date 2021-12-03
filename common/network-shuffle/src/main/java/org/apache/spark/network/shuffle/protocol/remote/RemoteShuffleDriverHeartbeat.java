package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RemoteShuffleDriverHeartbeat  extends BlockTransferMessage {
    private final String appId;
    private final int attempt;
    private final long latestHeartbeatTime;

    public RemoteShuffleDriverHeartbeat(String appId, int attempt, long latestHeartbeatTime) {
        this.appId = appId;
        this.attempt = attempt;
        this.latestHeartbeatTime = latestHeartbeatTime;
    }

    public String getAppId() {
        return appId;
    }

    public int getAttempt() {
        return attempt;
    }

    public long getLatestHeartbeatTime() {
        return latestHeartbeatTime;
    }

    public String getKey() {
        return appId + "_" + attempt;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + 4
                + 8;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        buf.writeInt(attempt);
        buf.writeLong(latestHeartbeatTime);
    }

    @Override
    protected Type type() {
        return Type.REMOTE_SHUFFLE_DRIVER_HEARTBEAT;
    }

    public static RemoteShuffleDriverHeartbeat decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        int attempt = buf.readInt();
        long latestHeartbeatTime = buf.readLong();
        return new RemoteShuffleDriverHeartbeat(appId, attempt, latestHeartbeatTime);
    }
}
