package org.apache.spark.remote.shuffle.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkTracker {
    public static final Logger LOG = LoggerFactory.getLogger(NetworkTracker.class);

    private static final String networkPath = "/proc/net/dev";
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


//    public static synchronized void collectNetworkInfoFromAccess(
//            RandomAccessFile randomAccessFile) {
//        try {
//            int length = 10 * 1024;
//            long inSize = 0, outSize = 0;
//            byte[] bs = new byte[length];
//            randomAccessFile.read(bs, 0, length);
//            randomAccessFile.seek(0);
//            String fileinfo = new String(bs);
//            List<String> lines = Arrays.asList(fileinfo.split("\n"));
//            for (String line : lines) {
//                line = line.trim();
//                if (line.length() == 0) {
//                    continue;
//                }
//                if (LOG.isDebugEnabled()) {
//                    LOG.debug("collectIops line is:" + line);
//                }
//                if (line.startsWith("eth0")
//                        || line.startsWith("eth1")
//                        || line.startsWith("eth2")
//                        || line.startsWith("eth3")) {
//                    String[] temp = line.split("\\s+");
//                    String netCard = temp[0];
//                    inSize = Long.parseLong(temp[1]); // Receive bytes,单位为Byte
//                    outSize = Long.parseLong(temp[9]); // Transmit bytes,单位为Byte
//                    NetMetric netMetric = new NetMetric(netCard, inSize, outSize, Time.monotonicNow());
//                    if (LOG.isDebugEnabled()) {
//                        LOG.debug("collect netMetric is:" + netMetric.toString());
//                    }
//                    currentNetMetricMap.put(netCard, netMetric);
//                }
//            }
//        } catch (Exception e) {
//            LOG.error(e.getMessage());
//        }
//        return currentNetMetricMap;
//    }
}
