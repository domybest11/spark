package org.apache.spark.remote.shuffle.metric;

import org.apache.spark.remote.shuffle.RemoteBlockHandler;
import org.apache.spark.remote.shuffle.WorkerMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkTracker {
    public static final Logger LOG = LoggerFactory.getLogger(NetworkTracker.class);

    private static RandomAccessFile randomAccessFile;

    static {
        try {
            String networkPath = "/proc/net/dev";
            File file = new File(networkPath);
            randomAccessFile = new RandomAccessFile(file, "r");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static synchronized void collectNetworkInfo(RemoteBlockHandler.WorkerMetrics workerMetrics) {
        try {
            int length = 10 * 1024;
            long inSize = 0, outSize = 0;
            byte[] bs = new byte[length];
            randomAccessFile.read(bs, 0, length);
            randomAccessFile.seek(0);
            String fileInfo = new String(bs);
            String[] lines = fileInfo.split("\n");
            int i = 0;
            for (String line : lines) {
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                if (line.startsWith("eth0")
                        || line.startsWith("eth1")
                        || line.startsWith("eth2")
                        || line.startsWith("eth3")) {
                    String[] temp = line.split("\\s+");
                    inSize += Long.parseLong(temp[1]); // Receive bytes,单位为Byte
                    outSize += Long.parseLong(temp[9]); // Transmit bytes,单位为Byte
                    workerMetrics.networkInGauge.update(i, inSize, System.currentTimeMillis());
                    workerMetrics.networkOutGauge.update(i, outSize, System.currentTimeMillis());
                    i++;
                }
            }
            workerMetrics.workerNetworkIn.mark(workerMetrics.networkInGauge.getValue());
            workerMetrics.workerNetworkOut.mark(workerMetrics.networkOutGauge.getValue());
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
