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

    public DiskInfo(String diskName, String path, DiskType diskType) {
        this.diskName = diskName;
        this.path = path;
        this.type = diskType;
        diskMetrics = new DiskMetrics();
    }


    public String getPath() {
        return path;
    }

    public String getDiskName() {
        return diskName;
    }

    public DiskType getType() {
        return type;
    }


    public static class DiskMetrics{
        public final IOGauge diskReadsCompleted = new IOGauge();
        public final IOGauge diskWritesCompleted = new IOGauge();
        public final IOUtilsGauge diskIOTime = new IOUtilsGauge();

        public final Meter diskRead = new Meter();
        public final Meter diskWrite = new Meter();
        public final Meter diskUtils = new Meter();

//        public DiskMetrics() {
//            allMetrics = new HashMap<>();
//            allMetrics.put("read", diskRead);
//            allMetrics.put("read_5min", (Gauge<Long>) () -> (long)diskRead.getFiveMinuteRate());
//            allMetrics.put("write", diskWrite);
//            allMetrics.put("write_5min", (Gauge<Long>) () -> (long)diskWrite.getFiveMinuteRate());
//            allMetrics.put("utils", diskUtils);
//            allMetrics.put("utils_5min", (Gauge<Long>) () -> (long)diskUtils.getFiveMinuteRate());
//        }

//        @Override
//        public Map<String, Metric> getMetrics() {
//            return allMetrics;
//        }

        public static String[] metricGetters = {
                "read",
                "read_5min",
                "write",
                "write_5min",
                "utils",
                "utils_5min"
        };
    }
}
