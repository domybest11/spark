package org.apache.spark.network.shuffle.protocol.remote;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RegisterWorker extends BlockTransferMessage {
    private final String host;
    private final int port;

    public RegisterWorker(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(host)
                + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, host);
        buf.writeInt(port);
    }

    @Override
    protected Type type() {
        return Type.REGISTER_WORKER;
    }

    public static RegisterWorker decode(ByteBuf buf) {
        String host = Encoders.Strings.decode(buf);
        int port = buf.readInt();
        return new RegisterWorker(host, port);
    }
}
