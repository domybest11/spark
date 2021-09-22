package org.apache.spark.remote.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class UnregisteredApplication extends BlockTransferMessage {
    private String appId;
    private int attempt;

    public UnregisteredApplication(String appId, int attempt) {
        this.appId = appId;
        this.attempt = attempt;
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
