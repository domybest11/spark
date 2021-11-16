package org.apache.spark.remote.shuffle.metric;

import org.apache.spark.remote.shuffle.DiskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Optional;
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

    public static void collectIOInfo(DiskInfo[] diskInfos, int interval) {
        try {
            int length = 10 * 1024;
            byte[] bs = new byte[length];
            randomAccessFile.read(bs, 0, length);
            randomAccessFile.seek(0);
            String fileInfo =  new String(bs);
            String[] lines = fileInfo.split("\n");
            for (String line : lines) {
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                String[] infos = line.trim().split("\\s+");
                String deviceName = infos[2];
                Stream<DiskInfo> diskInfoStream = Arrays.stream(diskInfos).filter((diskInfo) -> diskInfo.getDiskName().equals(deviceName));
                Optional<DiskInfo> diskInfoOptional = diskInfoStream.findFirst();
                if (!diskInfoOptional.isPresent()) {
                    continue;
                }
                long readsCompleted = Long.parseLong(infos[3]);
                long writesCompleted = Long.parseLong(infos[7]);
                long ioTimeMs = Long.parseLong(infos[12]);
                DiskInfo diskInfo = diskInfoOptional.get();
                diskInfo.diskMetrics.diskReadsCompleted.update(readsCompleted, System.currentTimeMillis());
                diskInfo.diskMetrics.diskWritesCompleted.update(writesCompleted, System.currentTimeMillis());
                diskInfo.diskMetrics.diskIOTime.update(ioTimeMs, System.currentTimeMillis());
                diskInfo.diskMetrics.diskRead.mark(diskInfo.diskMetrics.diskReadsCompleted.getValue() * interval);
                diskInfo.diskMetrics.diskWrite.mark(diskInfo.diskMetrics.diskWritesCompleted.getValue() * interval);
                diskInfo.diskMetrics.diskUtils.mark(diskInfo.diskMetrics.diskIOTime.getValue() * interval);
                File f = new File(diskInfo.getPath());
                diskInfo.diskMetrics.diskSpaceAvailable.update((long)(1.0 * f.getUsableSpace() / f.getTotalSpace() * 100));
            }

            Process process = Runtime.getRuntime().exec("df -i");
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() == 0) {
                    continue;
                }
                String[] df = line.trim().split("\\s+");
                String deviceName = df[0].replace("/dev/","");
                Stream<DiskInfo> diskInfoStream = Arrays.stream(diskInfos).filter((diskInfo) -> diskInfo.getDiskName().equals(deviceName));
                Optional<DiskInfo> diskInfoOptional = diskInfoStream.findFirst();
                if (!diskInfoOptional.isPresent()) {
                    continue;
                }
                DiskInfo diskInfo = diskInfoOptional.get();
                int ratio = Integer.parseInt(df[4].replace("%", ""));
                diskInfo.diskMetrics.diskInodeAvailable.update(ratio);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

}
