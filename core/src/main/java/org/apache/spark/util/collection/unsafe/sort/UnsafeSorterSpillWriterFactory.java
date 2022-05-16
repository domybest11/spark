package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.storage.BlockManager;

import java.io.IOException;

public class UnsafeSorterSpillWriterFactory {

    public static UnsafeSpillWriter createUnsafeSorterSpillWriter (
            SparkConf conf, BlockManager blockManager, int fileBufferSize,
            ShuffleWriteMetrics writeMetrics,
                int numRecordsToWrite, TaskContext context) throws IOException {
        UnsafeSpillWriter unsafeSpillWriter = null;
        if ((boolean) conf.get(package$.MODULE$.SHUFFLE_SPILL_REMOTE_ENABLE())) {
            String storageType = conf.get(package$.MODULE$.SHUFFLE_SPILL_STOREA_TYPE());
            if(storageType.equals("disk")) {
                unsafeSpillWriter =
                        new UnsafeSorterSpillWriter(blockManager, fileBufferSize, writeMetrics,
                        numRecordsToWrite);
            } else if (storageType.equals("hdfs")) {
                unsafeSpillWriter =
                 new UnsafeSorterHdfsSpillWriter(blockManager, fileBufferSize, writeMetrics,
                        numRecordsToWrite, context);
            } else {
                // other storage type uses disk type
                unsafeSpillWriter =
                new UnsafeSorterSpillWriter(blockManager, fileBufferSize, writeMetrics,
                        numRecordsToWrite);
            }
        } else {
            unsafeSpillWriter =
             new UnsafeSorterSpillWriter(blockManager, fileBufferSize, writeMetrics,
                    numRecordsToWrite);
        }
        return unsafeSpillWriter;
    }
}
