package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class CleanApplication extends BlockTransferMessage {
    private String appId;
    private int attempt;

    public CleanApplication(String appId, int attempt) {
        this.appId = appId;
        this.attempt = attempt;
    }

    public String getAppId() {
        return appId;
    }

    public int getAttempt() {
        return attempt;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
            + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        buf.writeInt(attempt);
    }

    @Override
    protected Type type() {
        return Type.CLEAN_APPLICATION;
    }

    public static CleanApplication decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        int attempt = buf.readInt();
        return new CleanApplication(appId, attempt);
    }
}
