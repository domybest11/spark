package org.apache.spark.remote.shuffle.metric;

import org.apache.spark.remote.shuffle.DiskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class IOStatusTracker {
    public static final Logger LOG = LoggerFactory.getLogger(IOStatusTracker.class);
    private static RandomAccessFile randomAccessFile;

    static {
        try {
            String diskStatusFile = "/proc/diskstats";
            File file = new File(diskStatusFile);
            randomAccessFile = new RandomAccessFile(file, "r");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void collectIOInfo(DiskInfo[] diskInfos) {
        try {
            int length = 10 * 1024;
            byte[] bs = new byte[length];
            randomAccessFile.read(bs, 0, length);
            randomAccessFile.seek(0);
            String fileInfo = new String(bs);
            String[] lines = fileInfo.split("\n");
            for (String line : lines) {
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                String[] infos = line.trim().split("\\s+");
                String deviceName = infos[2];
                Stream<DiskInfo> diskInfoStream = Arrays.stream(diskInfos).filter((diskInfo) -> diskInfo.getDiskName().equals("/dev/" + deviceName));
                if (!diskInfoStream.findFirst().isPresent()) {
                    continue;
                }
                long readsCompleted = Long.parseLong(infos[3]);
                long writesCompleted = Long.parseLong(infos[7]);
                long ioTimeMs = Long.parseLong(infos[12]);
                DiskInfo diskInfo = diskInfoStream.findFirst().get();
                diskInfo.diskMetrics.diskReadsCompleted.update(readsCompleted, System.currentTimeMillis());
                diskInfo.diskMetrics.diskWritesCompleted.update(writesCompleted, System.currentTimeMillis());
                diskInfo.diskMetrics.diskIOTime.update(ioTimeMs, System.currentTimeMillis());
                diskInfo.diskMetrics.diskRead.mark(diskInfo.diskMetrics.diskReadsCompleted.getValue());
                diskInfo.diskMetrics.diskWrite.mark(diskInfo.diskMetrics.diskWritesCompleted.getValue());
                diskInfo.diskMetrics.diskUtils.mark(diskInfo.diskMetrics.diskIOTime.getValue());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    public static ConcurrentHashMap collectDiskInfo() {
        ConcurrentHashMap<String, String> diskMap = new ConcurrentHashMap<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader("/etc/mtab"));
            String line;
            while ((line = reader.readLine()) != null) {
                LOG.info("collect diskMap line is:" + line);
                String[] sinfos = line.trim().split("\\s+");
                String deviceName = sinfos[0];
                String mountPoint = sinfos[1];
                if (mountPoint.contains("/mnt/storage")) {
                    diskMap.put(deviceName, mountPoint);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
        return diskMap;
    }

}
