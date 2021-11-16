package org.apache.spark.remote.shuffle;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class DiskManager {
    private static final Logger logger = LoggerFactory.getLogger(RemoteBlockHandler.class);
    private TransportConf conf;
    private final int subDirsPerLocalDir;
    public final DiskInfo[] workDirs;
    private Double diskIoUtilThreshold;
    private Double diskSpaceThreshold;
    private Double diskInodeThreshold;
    private int retainDiskNum;
    // key: appid_attempt  value: mergePath
    private final ConcurrentHashMap<String, List<String>> localMergeDirs = new ConcurrentHashMap<>();

    private final ExecutorService mergedDirCleaner = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("shuffle-merge-worker-directory-cleaner")
                    .build());

    public DiskManager(TransportConf conf) throws IOException {
        this.conf = conf;
        this.diskIoUtilThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskIOUtils","0.9"));
        this.diskSpaceThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskSpace","0.1"));
        this.diskInodeThreshold = Double.parseDouble(conf.get("spark.shuffle.worker.diskInode","0.1"));
        this.retainDiskNum = Integer.parseInt(conf.get("spark.shuffle.worker.RetainDiskNum","5"));
        this.subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64);
        this.retainDiskNum = Integer.parseInt(conf.get("spark.shuffle.worker.RetainDiskNum","5"));
        ProcessBuilder diskTypeProcess = new ProcessBuilder().command(
                "lsblk", "-d", "-o", "name,rota");
        Process proc = diskTypeProcess.start();
        Map<String, DiskType> diskTypeMap = new HashMap<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("s")) {
                String[] diskInfo = line.split("\\s+");
                if (diskInfo.length == 2) {
                    String deviceName = diskInfo[0];
                    DiskType diskType = DiskType.toDiskType(diskInfo[1]);
                    diskTypeMap.put(deviceName, diskType);
                }
            }
        }
        reader = new BufferedReader(new FileReader("/etc/mtab"));
        List<DiskInfo> diskInfos = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            String[] sinfos = line.trim().split("\\s+");
            String deviceName = sinfos[0].replace("/dev/", "");
            String mountPoint = sinfos[1];
            if (mountPoint.contains("/mnt/storage")) {
                DiskType diskType = diskTypeMap.getOrDefault(deviceName,DiskType.HDD);
                diskInfos.add(new DiskInfo(deviceName, mountPoint, diskType));
            }
        }
        workDirs = diskInfos.toArray(new DiskInfo[0]);
        reader.close();
        proc.destroy();
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
            if (!mergeDir.exists()) {
                for (int dirNum = 0; dirNum < subDirsPerLocalDir; dirNum++) {
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
            if (mergeDir.exists()) {
                localDirs.add(mergeDir.getAbsolutePath());
            }
        });
        localMergeDirs.putIfAbsent(appKey, localParentDirs);
        return localDirs.toArray(new String[0]);
    }

    public void cleanApplication(String appId, int attemptId) {
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
                        "Failed to create directory " + dirToCreate.getAbsolutePath() + " with permission 770 after 3 attempts!");
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
        localDirs.forEach(dir -> {
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
                && path.endsWith(appAttempt);
    }


    private List<DiskInfo> chooseDir() {
        List<DiskInfo> diskInfos = Arrays.asList(workDirs);
        Iterator<DiskInfo> iterator = diskInfos.iterator();
        while (iterator.hasNext()) {
            DiskInfo diskInfo = iterator.next();
            double diskIOUtils = 1.0 * diskInfo.diskMetrics.diskIOTime.getValue() / 100;
            double diskSpaceAvailable = 1.0 * diskInfo.diskMetrics.diskSpaceAvailable.getValue() / 100;
            double diskInodeAvailable = 1.0 * diskInfo.diskMetrics.diskInodeAvailable.getValue() / 100;
            if (diskIOUtils > diskIoUtilThreshold ||
                    diskSpaceAvailable < diskSpaceThreshold || diskInodeAvailable < diskInodeThreshold) {
                iterator.remove();
            }
        }
        if (diskInfos.size() < retainDiskNum) {
            return Arrays.asList(workDirs);
        } else {
            return diskInfos;
        }
    }

}
