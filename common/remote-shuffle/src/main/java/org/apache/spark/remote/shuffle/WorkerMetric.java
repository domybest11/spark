package org.apache.spark.remote.shuffle;

import com.codahale.metrics.*;
import org.apache.spark.remote.shuffle.metric.IOGauge;
import org.apache.spark.remote.shuffle.metric.NetworkGauge;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class WorkerMetric implements MetricSet {

    private final Map<String, Metric> allMetrics;
    OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
    private final Gauge<Long> workerCpuAvailable = () -> (long) osmxb.getAvailableProcessors();
    private final Gauge<Long> workerCpuLoadAverage = () -> {
        double v = osmxb.getSystemLoadAverage();
        System.out.println(v);
        return (long) v;
    };
    public final NetworkGauge networkInGauge = new NetworkGauge(4);
    public final NetworkGauge networkOutGauge = new NetworkGauge(4);


    public final Meter workerNetworkIn = new Meter();
    public final Meter workerNetworkOut = new Meter();

    private final Counter workerAliveConnection = new Counter();

    public WorkerMetric() {
        allMetrics = new HashMap<>();
        allMetrics.put("workerCpuLoadAverage", workerCpuLoadAverage);
        allMetrics.put("workerCpuAvailable", workerCpuAvailable);
        allMetrics.put("workerNetworkIn", workerNetworkIn);
        allMetrics.put("workerNetworkOut", workerNetworkOut);
        allMetrics.put("workerNetworkIn_5min", (Gauge<Long>) () -> (long) workerNetworkIn.getFiveMinuteRate());
        allMetrics.put("workerNetworkOut_5min", (Gauge<Long>) () -> (long) workerNetworkOut.getFiveMinuteRate());
        allMetrics.put("workerAliveConnection", workerAliveConnection);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return allMetrics;
    }

    public long[] getCurrentMetrics() {
        int diskNums = 1;
        long[] metrics = new long[7 + diskNums * 3 + 1];
        return metrics;
    }

    public String[] metricGetters = {
            "workerCpuLoadAverage",
            "workerCpuAvailable",
            "workerNetworkIn",
            "workerNetworkIn_5min",
            "workerNetworkOut",
            "workerNetworkOut_5min",
            "workerAliveConnection"
    };


    public static void main(String[] args) throws Exception {
        WorkerMetric x = new WorkerMetric();


        while (true) {
            System.out.println(x.workerCpuAvailable.getValue());
            Thread.sleep(1000L);
        }
    }
}
