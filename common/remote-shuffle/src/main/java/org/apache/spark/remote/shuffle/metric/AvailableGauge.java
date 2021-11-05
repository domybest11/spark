package org.apache.spark.remote.shuffle.metric;

import com.codahale.metrics.Gauge;

public class AvailableGauge implements Gauge<Long> {
    private long value = 0L;

    public void update(long value) {
        this.value = value;
    }

    @Override
    public Long getValue() {
        return value;
    }
}
