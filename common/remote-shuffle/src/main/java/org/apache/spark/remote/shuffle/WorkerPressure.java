package org.apache.spark.remote.shuffle;

public class WorkerPressure implements Comparable {

    private long cpuLoadAverage;
    private long cpuAvailable;
    private long networkInBytes;
    private long networkInBytes5min;
    private long networkOutBytes;
    private long networkOutBytes5min;
    private long aliveConnection;
    private long diskInfo[][];


    /*
        0:"workerCpuLoadAverage",
        1:"workerCpuAvailable",
        2:"workerNetworkIn",
        3:"workerNetworkIn_5min",
        4:"workerNetworkOut",
        5:"workerNetworkOut_5min",
        6:"workerAliveConnection"
        7:disk_1:"read",
        8:disk_1:"write",
        9:disk_1:"utils"
        ...
        n-4:disk_diskNum:"read",
        n-3:disk_diskNum:"write",
        n-2:disk_diskNum:"utils"
        n-1: diskNum
     */
    public WorkerPressure(long[] workerMetrics) {
        cpuLoadAverage = workerMetrics[0];
        cpuAvailable = workerMetrics[1];
        networkInBytes = workerMetrics[2];
        networkInBytes5min = workerMetrics[3];
        networkOutBytes = workerMetrics[4];
        networkOutBytes5min = workerMetrics[5];
        aliveConnection = workerMetrics[6];
        int num = (int) workerMetrics[workerMetrics.length-1];
        diskInfo = new long[num][3];

    }

    // TODO: 2021/11/1 根据指标进行available判断
    public boolean available() {
        return true;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

}
