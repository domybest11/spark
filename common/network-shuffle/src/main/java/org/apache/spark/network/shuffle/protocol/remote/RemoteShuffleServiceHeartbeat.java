package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RemoteShuffleServiceHeartbeat extends BlockTransferMessage {

    private final String host;
    private final int port;
    private final long heartbeatTimeMs;
    private final String pressure;
    private final RunningStage[] runningStages;

    public RemoteShuffleServiceHeartbeat(String host, int port, long heartbeatTimeMs, String pressure, RunningStage[] runningStages) {
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

    public String getPressure() {
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
                + Encoders.Strings.encodedLength(pressure)
                + encodedLengthOfRunningStage;
    }

    @Override
    public void encode(ByteBuf buf) {

    }


}
