package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class GetPushMergerLocations extends BlockTransferMessage {
    private String appId;
    private int attempt;
    private int shuffleId;
    private int numPartitions;
    private int tasksPerExecutor;
    private int maxExecutors;

    public GetPushMergerLocations(String appId, int attempt, int shuffleId, int numPartitions, int tasksPerExecutor, int maxExecutors) {
        this.appId = appId;
        this.attempt = attempt;
        this.shuffleId = shuffleId;
        this.numPartitions = numPartitions;
        this.tasksPerExecutor = tasksPerExecutor;
        this.maxExecutors = maxExecutors;
    }

    public String getAppId() {
        return appId;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getTasksPerExecutor() {
        return tasksPerExecutor;
    }

    public int getMaxExecutors() {
        return maxExecutors;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) + 4 * 5;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        buf.writeInt(attempt);
        buf.writeInt(shuffleId);
        buf.writeInt(numPartitions);
        buf.writeInt(tasksPerExecutor);
        buf.writeInt(maxExecutors);
    }

    @Override
    protected Type type() {
        return Type.GET_PUSH_MERGER_LOCATIONS;
    }

    public static GetPushMergerLocations decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        int attempt = buf.readInt();
        int shuffleId = buf.readInt();
        int numPartitions = buf.readInt();
        int tasksPerExecutor = buf.readInt();
        int maxExecutors = buf.readInt();
        return new GetPushMergerLocations(appId, attempt, shuffleId, numPartitions, tasksPerExecutor, maxExecutors);
    }
}
