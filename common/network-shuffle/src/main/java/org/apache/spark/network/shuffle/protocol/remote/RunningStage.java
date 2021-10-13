package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

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
        return Encoders.Strings.encodedLength(applicationId)
                + 4
                + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, applicationId);
        buf.writeInt(attemptId);
        buf.writeInt(shuffleId);
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

    public static RunningStage decode(ByteBuf buf) {
        String applicationId = Encoders.Strings.decode(buf);
        int attemptId = buf.readInt();
        int shuffleId = buf.readInt();
        return new RunningStage(applicationId, attemptId, shuffleId);
    }

}
