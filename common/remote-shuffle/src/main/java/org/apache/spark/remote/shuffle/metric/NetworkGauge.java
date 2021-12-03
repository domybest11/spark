package org.apache.spark.remote.shuffle.metric;

import com.codahale.metrics.Gauge;

public class NetworkGauge implements Gauge<Long> {
    private long[] pre;
    private long[] cur;
    private long[] preTime;
    private long[] curTime;

    public NetworkGauge(int ethNum) {
        pre = new long[ethNum];
        cur = new long[ethNum];
        preTime = new long[ethNum];
        curTime = new long[ethNum];
    }

    public void update(int idx, long curValue, long time) {
        if (idx < pre.length) {
            pre[idx] = cur[idx];
            cur[idx] = curValue;
            preTime[idx] = curTime[idx];
            curTime[idx] = time;
        }
    }

    @Override
    public Long getValue() {
        long value = 0L;
        for (int i = 0; i < pre.length; i++) {
            if (pre[i] != 0) {
                if ((cur[i] - pre[i]) != 0) {
                    value += (long)(1.0 * (cur[i] - pre[i]) / (curTime[i] - preTime[i]) * 1000);
                }
            }
        }
        return value * 8 / 1000000;
    }
}
