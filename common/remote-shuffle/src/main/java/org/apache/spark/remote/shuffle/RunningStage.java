package org.apache.spark.remote.shuffle;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encodable;

public class RunningStage implements Encodable {
    private final String applicationId;
    private final int attemptId;
    private final int shuffleId;

    public RunningStage(String applicationId, int attemptId, int shuffleId) {
        this.applicationId = applicationId;
        this.attemptId = attemptId;
        this.shuffleId = shuffleId;
    }

    @Override
    public int encodedLength() {
        return 0;
    }

    @Override
    public void encode(ByteBuf buf) {

    }

    public String getApplicationId() {
        return applicationId;
    }

    public int getAttemptId() {
        return attemptId;
    }

    public int getShuffleId() {
        return shuffleId;
    }

}
