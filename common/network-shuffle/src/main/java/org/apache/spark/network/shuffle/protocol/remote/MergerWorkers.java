package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class MergerWorkers extends BlockTransferMessage {

    private final String[] workerInfos;

    public MergerWorkers(String[] workerInfos) {
        this.workerInfos = workerInfos;
    }

    public String[] getWorkerInfos() {
        return workerInfos;
    }

    @Override
    public int encodedLength() {
        return Encoders.StringArrays.encodedLength(workerInfos);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.StringArrays.encode(buf, workerInfos);
    }

    @Override
    protected Type type() {
        return Type.MERGER_WORKERS;
    }

    public static MergerWorkers decode(ByteBuf buf) {
        String[] workerInfos = Encoders.StringArrays.decode(buf);
        return new MergerWorkers(workerInfos);
    }
}
