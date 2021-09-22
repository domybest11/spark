package org.apache.spark.remote.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RegisteredApplication extends BlockTransferMessage {
    private String appId;
    private int attempt;

    public RegisteredApplication(String appId, int attempt) {
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
