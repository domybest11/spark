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
        if (pre != 0) {
            if ((cur - pre) != 0) {
                return (long)(1.0 * (cur - pre) / (curTime - preTime) * 100);
            }
        }
        return 0L;
    }
}
