package org.apache.spark.remote.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.remote.shuffle.RunningStage;
import org.apache.spark.remote.shuffle.WorkerPressure;

public class RemoteShuffleServiceHeartbeat extends BlockTransferMessage {

    private final String host;
    private final int port;
    private final long heartbeatTimeMs;
    private final WorkerPressure pressure;
    private final RunningStage[] runningStages;

    public RemoteShuffleServiceHeartbeat(String host, int port, long heartbeatTimeMs, WorkerPressure pressure, RunningStage[] runningStages) {
        this.host = host;
        this.port = port;
        this.heartbeatTimeMs = heartbeatTimeMs;
        this.pressure = pressure;
        this.runningStages = runningStages;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public WorkerPressure getPressure() {
        return pressure;
    }

    public long getHeartbeatTimeMs() {
        return heartbeatTimeMs;
    }

    public RunningStage[] getRunningStages() {
        return runningStages;
    }

    @Override
    protected Type type() {
        return Type.SHUFFLE_HEARTBEAT;
    }

    @Override
    public int encodedLength() {
        int encodedLengthOfRunningStage = 0;
        for (RunningStage runningStage: runningStages) {
            encodedLengthOfRunningStage += runningStage.encodedLength();
        }
        return Encoders.Strings.encodedLength(host)
                + 4 // port
                + 8 // heartbeatTimeoutMs
                + pressure.encodedLength()
                + encodedLengthOfRunningStage;
    }

    @Override
    public void encode(ByteBuf buf) {

    }


}
