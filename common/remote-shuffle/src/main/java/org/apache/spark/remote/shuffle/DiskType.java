package org.apache.spark.remote.shuffle;

import java.util.Locale;

public enum DiskType {
    SSD,HDD,NVME;

    public static DiskType toDiskType(String type) {
        switch (type.toUpperCase(Locale.ROOT)) {
            case "SSD" : return SSD;
            case "NVME" : return NVME;
            case "0" : return SSD;
            default: return HDD;
        }
    }
}
