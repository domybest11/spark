package org.apache.spark.remote.shuffle;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

public class WorkerPressure implements Comparable, Encodable {
    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public int encodedLength() {
        return 0;
    }

    @Override
    public void encode(ByteBuf buf) {

    }
}
