package org.apache.spark.remote.shuffle;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encodable;

public class RunningStage implements Encodable {
    private final String applicationId;
    private final int attemptId;
    private final int shuffleId;
    private final int shuffleMergeId;
    private final int numMappers;
    private final int numPartitions;
    private final int mergePartitions;

    public RunningStage(String applicationId, int attemptId, int shuffleId, int shuffleMergeId, int numMappers, int numPartitions, int mergePartitions) {
        this.applicationId = applicationId;
        this.attemptId = attemptId;
        this.shuffleId = shuffleId;
        this.shuffleMergeId = shuffleMergeId;
        this.numMappers = numMappers;
        this.numPartitions = numPartitions;
        this.mergePartitions = mergePartitions;
    }

    @Override
    public int encodedLength() {
        return 0;
    }

    @Override
    public void encode(ByteBuf buf) {

    }
}
