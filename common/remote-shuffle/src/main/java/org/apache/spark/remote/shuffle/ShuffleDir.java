package org.apache.spark.remote.shuffle;

public class ShuffleDir {
    private String path;
    private DiskType type;
    private int io;
    private long sampleTime;

    public ShuffleDir(String path, DiskType type) {
        this.path = path;
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public DiskType getType() {
        return type;
    }

    public int getIo() {
        return io;
    }

    public long getSampleTime() {
        return sampleTime;
    }
}
