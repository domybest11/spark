package org.apache.spark.remote.shuffle.metric;

import com.codahale.metrics.Gauge;

public class IOUtilsGauge implements Gauge<Long> {
    private long pre;
    private long cur;
    private long preTime;
    private long curTime;

    public void update(long curValue, long time) {
        pre = cur;
        cur = curValue;
        preTime = curTime;
        curTime = time;
    }

    @Override
    public Long getValue() {
        if (cur != 0) {
            return (cur - pre) / (curTime - preTime) * 100;
        }
        return 0L;
    }
}
