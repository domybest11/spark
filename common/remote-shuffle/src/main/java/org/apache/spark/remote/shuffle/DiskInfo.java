package org.apache.spark.remote.shuffle;

import com.codahale.metrics.*;
import org.apache.spark.remote.shuffle.metric.IOGauge;
import org.apache.spark.remote.shuffle.metric.IOUtilsGauge;

import java.util.HashMap;
import java.util.Map;

public class DiskInfo {
    private String path;
    private String diskName;
    private DiskType type;
    public DiskMetrics diskMetrics;
    private long sampleTime;

    public DiskInfo(String path, DiskType type) {
        this.path = path;
        this.type = type;
        diskMetrics = new DiskMetrics();
    }

    public void setSampleTime(long sampleTime) {
        this.sampleTime = sampleTime;
    }

    public String getPath() {
        return path;
    }

    public DiskType getType() {
        return type;
    }

    public long getSampleTime() {
        return sampleTime;
    }

    public static class DiskMetrics implements MetricSet {
        private final Map<String, Metric> allMetrics;

        public final IOGauge diskReadsCompleted = new IOGauge();
        public final IOGauge diskWritesCompleted = new IOGauge();
        public final IOUtilsGauge diskIOTime = new IOUtilsGauge();

        public final Meter diskRead = new Meter();
        public final Meter diskWrite = new Meter();
        public final Meter diskUtils = new Meter();

        public DiskMetrics() {
            allMetrics = new HashMap<>();
            allMetrics.put("read", diskRead);
            allMetrics.put("read_5min", (Gauge<Long>) () -> (long)diskRead.getFiveMinuteRate());
            allMetrics.put("write", diskWrite);
            allMetrics.put("write_5min", (Gauge<Long>) () -> (long)diskWrite.getFiveMinuteRate());
            allMetrics.put("utils", diskUtils);
            allMetrics.put("utils_5min", (Gauge<Long>) () -> (long)diskUtils.getFiveMinuteRate());
        }

        @Override
        public Map<String, Metric> getMetrics() {
            return allMetrics;
        }

        public static String[] metricGetters = {
                "read",
                "write",
                "utils"
        };
    }
}
