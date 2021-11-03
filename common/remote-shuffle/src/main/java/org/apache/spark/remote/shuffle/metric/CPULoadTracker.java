package org.apache.spark.remote.shuffle.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class CPULoadTracker {
    public static final Logger LOG = LoggerFactory.getLogger(CPULoadTracker.class);

    public static class CpuLoadMetric {
        private final double cpuLoad;
        private final int cpuProcessor;

        CpuLoadMetric(final double cpuLoad, final int cpuProcessor) {
            this.cpuLoad = cpuLoad;
            this.cpuProcessor = cpuProcessor;
        }

        public double getCpuLoad() {
            return cpuLoad;
        }

        public int getCpuProcessor() {
            return cpuProcessor;
        }
    }

    public static CpuLoadMetric collectCpuLoad() {
        OperatingSystemMXBean osmxb =
                (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        double cpuLoad = osmxb.getSystemLoadAverage();
        CpuLoadMetric cpuLoadMetric = new CpuLoadMetric(cpuLoad, osmxb.getAvailableProcessors());
        return cpuLoadMetric;
    }
}
