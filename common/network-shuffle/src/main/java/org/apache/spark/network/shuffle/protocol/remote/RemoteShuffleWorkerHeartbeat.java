package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RemoteShuffleWorkerHeartbeat extends BlockTransferMessage {

    private final String host;
    private final int port;
    private final long heartbeatTimeMs;
    private final String pressure;
    private final RunningStage[] runningStages;

    public RemoteShuffleWorkerHeartbeat(String host, int port, long heartbeatTimeMs, String pressure, RunningStage[] runningStages) {
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
        return Encoders.Strings.encodedLength(host)
                + 4 // port
                + 8 // heartbeatTimeoutMs
                + Encoders.Strings.encodedLength(pressure)
                + RunningStages.encodedLength(runningStages);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, host);
        buf.writeInt(port);
        buf.writeLong(heartbeatTimeMs);
        Encoders.Strings.encode(buf, pressure);
        RunningStages.encode(buf, runningStages);
    }

    public static RemoteShuffleWorkerHeartbeat decode(ByteBuf buf) {
        String host = Encoders.Strings.decode(buf);
        int port = buf.readInt();
        long heartbeatTimeMs = buf.readLong();
        String pressure = Encoders.Strings.decode(buf);
        RunningStage[] runningStages = RunningStages.decode(buf);
        return new RemoteShuffleWorkerHeartbeat(host, port, heartbeatTimeMs, pressure, runningStages);
    }


    private static class RunningStages {
        public static int encodedLength(RunningStage[] runningStages) {
            int totalLength = 4;
            for (RunningStage runningStage : runningStages) {
                totalLength += runningStage.encodedLength();
            }
            return totalLength;
        }

        public static void encode(ByteBuf buf, RunningStage[] runningStages) {
            buf.writeInt(runningStages.length);
            for (RunningStage runningStage : runningStages) {
                runningStage.encode(buf);
            }
        }

        public static RunningStage[] decode(ByteBuf buf) {
            int numRunningStages = buf.readInt();
            RunningStage[] runningStages = new RunningStage[numRunningStages];
            for (int i = 0; i < runningStages.length; i ++) {
                runningStages[i] = RunningStage.decode(buf);
            }
            return runningStages;
        }
    }


}
