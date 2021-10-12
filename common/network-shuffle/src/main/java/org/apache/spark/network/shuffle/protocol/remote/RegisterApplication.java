package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RegisterApplication extends BlockTransferMessage {
    private String appId;
    private int attempt;

    public RegisterApplication(String appId, int attempt) {
        this.appId = appId;
        this.attempt = attempt;
    }

    public String getAppId() {
        return appId;
    }

    public int getAttempt() {
        return attempt;
    }

    public String getKey() {
        return appId + "_" + attempt;
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
        return Type.REGISTER_APPLICATION;
    }

    public static RegisterApplication decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        int attempt = buf.readInt();
        return new RegisterApplication(appId, attempt);
    }
}
