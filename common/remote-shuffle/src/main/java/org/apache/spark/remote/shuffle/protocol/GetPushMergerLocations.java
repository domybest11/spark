package org.apache.spark.remote.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class GetPushMergerLocations extends BlockTransferMessage {
    private String appId;
    private int attempt;
    private int shuffleId;
    private int numPartitions;
    private int resourceProfileId;

    public GetPushMergerLocations(String appId, int attempt, int shuffleId, int numPartitions, int resourceProfileId) {
        this.appId = appId;
        this.attempt = attempt;
        this.shuffleId = shuffleId;
        this.numPartitions = numPartitions;
        this.resourceProfileId = resourceProfileId;
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
