package org.apache.spark.remote.shuffle;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.spark.remote.shuffle.DiskInfo.DiskMetrics.metricGetters;

public class DiskManager {
    private static final Logger logger = LoggerFactory.getLogger(RemoteBlockHandler.class);
    private final TransportConf conf;
    private final int subDirsPerLocalDir;
    public final DiskInfo[] workDirs;
    // key: appid_attempt  value: mergePath
    private final ConcurrentHashMap<String, List<String>> localMergeDirs = new ConcurrentHashMap<>();

    private final ScheduledExecutorService pressureMonitorThread =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("pressure-monitor")
                            .build());

    private final ExecutorService mergedDirCleaner = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("shuffle-merge-worker-directory-cleaner")
                    .build());

    public DiskManager(TransportConf conf) throws IOException {
        this.conf = conf;
        long monitorInterval = JavaUtils.timeStringAsSec(conf.get("spark.shuffle.remote.worker.monitor", "60s"));
        this.subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories",64);
        String dirConf = conf.get("spark.shuffle.remote.worker.dirs", "");
        if (StringUtils.isBlank(dirConf)) {
            throw new IOException("Remote shuffle worker dirs is empty");
        } else {
            String[] dirs = dirConf.split(",");
            workDirs = new DiskInfo[dirs.length];
            for(int i=0; i < dirs.length; i++) {
                if (dirs[i].contains(":")) {
                    String[] arr = dirs[i].split(":");
                    String path = arr[0];
                    DiskType diskType = DiskType.toDiskType(arr[1]);
                    workDirs[i] = new DiskInfo(path, diskType);
                } else {
                    workDirs[i] = new DiskInfo(dirs[i], DiskType.HDD);
                }
            }
        }
        pressureMonitorThread.scheduleAtFixedRate(
                new IOMonitor(), 10, monitorInterval, TimeUnit.SECONDS);
    }

    public String[] makeMergeSpace(String appId, int attemptId) {
        String appKey = appId + "_" + attemptId;
        List<String> localDirs = new ArrayList<>();
        List<String> localParentDirs = new ArrayList<>();
        chooseDir().forEach(diskInfo -> {
            String rootDir = diskInfo.getPath();
            String parentPath = rootDir + "/" + appKey;
            localParentDirs.add(parentPath);
            File mergeDir = new File(rootDir, appKey + "/merge_manager");
            if(!mergeDir.exists()) {
                for(int dirNum = 0; dirNum < subDirsPerLocalDir; dirNum++) {
                    File subDir = new File(mergeDir, String.format("%02x", dirNum));
                    try {
                        if (!subDir.exists()) {
                            // Only one container will create this directory. The filesystem will handle
                            // any race conditions.
                            createDirWithPermission770(subDir);
                        }
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(mergeDir.exists()){
                localDirs.add(mergeDir.getAbsolutePath());
            }
        });
        localMergeDirs.putIfAbsent(appKey, localParentDirs);
        return localDirs.toArray(new String[0]);
    }

    public void cleanApplication (String appId, int attemptId) {
        String appKey = appId + "_" + attemptId;
        List<String> localDirs = localMergeDirs.remove(appKey);
        if (!localDirs.isEmpty()) {
            mergedDirCleaner.execute(() ->
                    deleteExecutorDirs(localDirs, appKey));
        }
    }



    private void createDirWithPermission770(File dirToCreate) throws IOException, InterruptedException {
        int attempts = 0;
        int maxAttempts = 3;
        File created = null;
        while (created == null) {
            attempts += 1;
            if (attempts > maxAttempts) {
                throw new IOException(
                        "Failed to create directory "+ dirToCreate.getAbsolutePath() +" with permission 770 after 3 attempts!");
            }
            try {
                ProcessBuilder builder = new ProcessBuilder().command(
                        "mkdir", "-p", "-m770", dirToCreate.getAbsolutePath());
                Process proc = builder.start();
                int exitCode = proc.waitFor();
                if (dirToCreate.exists()) {
                    created = dirToCreate;
                }
            } catch (SecurityException e) {
                logger.warn("Failed to create directory " + dirToCreate.getAbsolutePath() + " with permission 770", e);
                created = null;
            }
        }
    }

    void deleteExecutorDirs(List<String> localDirs, String appAttempt) {
        localDirs.forEach(dir ->{
            Path localDir = Paths.get(dir);
            try {
                if (Files.exists(localDir)
                        && checkDeleteDirs(localDir.toAbsolutePath().toString(), appAttempt)) {
                    JavaUtils.deleteRecursively(localDir.toFile());
                    logger.info("Successfully cleaned up directory: {}", localDir);
                }
            } catch (Exception e) {
                logger.error("Failed to delete directory: {}", localDir, e);
            }
        });
    }

    public Boolean checkDeleteDirs(String path, String appAttempt) {
        return !StringUtils.isBlank(path) && !StringUtils.isBlank(appAttempt)
                && path.endsWith(appAttempt) ;
    }


    private List<DiskInfo> chooseDir() {
        // TODO: 2021/10/29 根据压力进行可用磁盘选择
        return Arrays.asList(workDirs);
    }

    public static void main(String[] args) throws IOException {
        DiskManager x = new DiskManager(null);
        IOMonitor ioMonitor = x.new IOMonitor();
    }

    private class IOMonitor implements Runnable {

        @Override
        public void run() {
            // TODO: 2021/9/26 监控磁盘压力
            logger.info("PressureMonitor");
            // TODO: 2021/10/29 各种节点指标监控
            ProcessBuilder iostat = new ProcessBuilder().command(
                    "iostat", "1", "1", "-d","-x");
            try {
                Process proc = iostat.start();
                BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("s")) {
                        String[] ioInfo = line.split("\\s+");
                        if(ioInfo.length == 14) {
                            String disk = ioInfo[0];
                            long read = (long)(Double.parseDouble(ioInfo[3]));
                            long write = (long)(Double.parseDouble(ioInfo[4]));
                            long await = (long)(Double.parseDouble(ioInfo[9]));
                            long util = (long)(Double.parseDouble(ioInfo[13]));
                            ((Meter)workDirs[0].diskMetrics.getMetrics().get(metricGetters[0])).mark(read);
                            ((Meter)workDirs[0].diskMetrics.getMetrics().get(metricGetters[1])).mark(write);
                            ((Meter)workDirs[0].diskMetrics.getMetrics().get(metricGetters[2])).mark(await);
                            ((Meter)workDirs[0].diskMetrics.getMetrics().get(metricGetters[3])).mark(util);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
