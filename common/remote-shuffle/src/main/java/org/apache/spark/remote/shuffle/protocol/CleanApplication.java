package org.apache.spark.remote.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class CleanApplication extends BlockTransferMessage {

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
