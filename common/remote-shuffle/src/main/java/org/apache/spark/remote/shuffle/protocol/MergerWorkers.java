package org.apache.spark.remote.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.remote.shuffle.WorkerInfo;

public class MergerWorkers extends BlockTransferMessage {

    private final WorkerInfo[] workerInfos;

    public MergerWorkers(WorkerInfo[] workerInfos) {
        this.workerInfos = workerInfos;
    }

    public WorkerInfo[] getWorkerInfos() {
        return workerInfos;
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
