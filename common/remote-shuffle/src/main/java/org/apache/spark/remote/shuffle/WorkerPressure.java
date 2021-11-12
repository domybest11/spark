package org.apache.spark.remote.shuffle;

import java.util.ArrayList;

public class WorkerPressure implements Comparable {

    public long cpuLoadAverage;
    public long cpuAvailable;
    public long networkInBytes;
    public long networkInBytes5min;
    public long networkOutBytes;
    public long networkOutBytes5min;
    public long aliveConnection;
    public long diskInfo[][];


    /*
        0:"workerCpuLoadAverage",  cpuload (1分钟平均)
        1:"workerCpuAvailable",    cpu 核数
        2:"workerNetworkIn",       网卡写入量（多网卡累加值，单位字节）
        3:"workerNetworkIn_5min",  网卡写入量（多网卡累加值 5分钟均值，单位字节）
        4:"workerNetworkOut",      网卡写出量（多网卡累加值，单位字节）
        5:"workerNetworkOut_5min", 网卡写出量（多网卡累加值 5分钟均值，单位字节）
        6:"workerAliveConnection"  连接到worker进行push的连接数
        7:disk_1:"read",           磁盘1 读取量（5分钟均值，单位字节）
        8:disk_1:"write",          磁盘1 写出量 (5分钟均值，单位字节）
        9:disk_1:"utils"           磁盘1 ioutils  (5分钟均值)
        ...
        n-4:disk_diskNum:"read",   磁盘n 读取量（5分钟均值，单位字节）
        n-3:disk_diskNum:"write",  磁盘n 写出量 (5分钟均值，单位字节）
        n-2:disk_diskNum:"utils"   磁盘n ioutils  (5分钟均值)
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
        diskInfo = new long[num][8];
        for(int i = 0; i < num; i++) {
            for(int j = 0; j < 8; j++) {
                diskInfo[i][j] = workerMetrics[7 + i * 8 + j];
            }
        }
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
