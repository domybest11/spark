/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection.unsafe.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.storage.*;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.internal.config.package$;

/**
 * Spills a list of sorted records to disk. Spill files have the following format:
 *
 *   [# of records (int)] [[len (int)][prefix (long)][data (bytes)]...]
 */
public final class UnsafeSorterHdfsSpillWriter extends UnsafeSpillWriter {

    private final SparkConf conf = SparkEnv.get().conf();

    private final int diskWriteBufferSize =
            (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());

    private byte[] writeBuffer = new byte[diskWriteBufferSize];

    private final Path path;
    private final BlockId blockId;
    private final Configuration hadoopConf;
    private final int numRecordsToWrite;
    private HdfsBlockObjectWriter writer;
    private int numRecordsSpilled = 0;

    public UnsafeSorterHdfsSpillWriter(
            BlockManager blockManager,
            int fileBufferSize,
            ShuffleWriteMetrics writeMetrics,
            int numRecordsToWrite,
            TaskContext context
           ) throws IOException {
        this.hadoopConf = new SparkHadoopUtil().newConfiguration(conf);
        final Tuple2<TempLocalBlockId, Path> spilledFileInfo =
                blockManager.diskBlockManager().createTempHdfsBlock(conf.get("spark.app.id"),
                        conf.get("spark.app.attempt.id", "0"),
                        context.stageId(), context.stageAttemptNumber(),
                        context.taskAttemptId(), context.attemptNumber(),
                        hadoopConf);
        this.path = spilledFileInfo._2();
        this.blockId = spilledFileInfo._1();

        this.numRecordsToWrite = numRecordsToWrite;
        writer = blockManager.getHdfsWriter(blockId, path, DummySerializerInstance.INSTANCE,
                fileBufferSize, writeMetrics, hadoopConf, conf);
        // Write the number of records
        writeIntToBuffer(numRecordsToWrite, 0);
        writer.write(writeBuffer, 0, 4);
    }

    // Based on DataOutputStream.writeLong.
    private void writeLongToBuffer(long v, int offset) {
        writeBuffer[offset + 0] = (byte)(v >>> 56);
        writeBuffer[offset + 1] = (byte)(v >>> 48);
        writeBuffer[offset + 2] = (byte)(v >>> 40);
        writeBuffer[offset + 3] = (byte)(v >>> 32);
        writeBuffer[offset + 4] = (byte)(v >>> 24);
        writeBuffer[offset + 5] = (byte)(v >>> 16);
        writeBuffer[offset + 6] = (byte)(v >>>  8);
        writeBuffer[offset + 7] = (byte)(v >>>  0);
    }

    private void writeIntToBuffer(int v, int offset) {
        writeBuffer[offset + 0] = (byte)(v >>> 24);
        writeBuffer[offset + 1] = (byte)(v >>> 16);
        writeBuffer[offset + 2] = (byte)(v >>>  8);
        writeBuffer[offset + 3] = (byte)(v >>>  0);
    }

    public void write(
            Object baseObject,
            long baseOffset,
            int recordLength,
            long keyPrefix) throws IOException {
        if (numRecordsSpilled == numRecordsToWrite) {
            throw new IllegalStateException(
                    "Number of records written exceeded numRecordsToWrite = " + numRecordsToWrite);
        } else {
            numRecordsSpilled++;
        }
        writeIntToBuffer(recordLength, 0);
        writeLongToBuffer(keyPrefix, 4);
        int dataRemaining = recordLength;
        int freeSpaceInWriteBuffer = diskWriteBufferSize - 4 - 8; // space used by prefix + len
        long recordReadPosition = baseOffset;
        while (dataRemaining > 0) {
            final int toTransfer = Math.min(freeSpaceInWriteBuffer, dataRemaining);
            Platform.copyMemory(
                    baseObject,
                    recordReadPosition,
                    writeBuffer,
                    Platform.BYTE_ARRAY_OFFSET + (diskWriteBufferSize - freeSpaceInWriteBuffer),
                    toTransfer);
            writer.write(writeBuffer, 0, (diskWriteBufferSize - freeSpaceInWriteBuffer) + toTransfer);
            recordReadPosition += toTransfer;
            dataRemaining -= toTransfer;
            freeSpaceInWriteBuffer = diskWriteBufferSize;
        }
        if (freeSpaceInWriteBuffer < diskWriteBufferSize) {
            writer.write(writeBuffer, 0, (diskWriteBufferSize - freeSpaceInWriteBuffer));
        }
        writer.recordWritten();
    }

    public void close() throws IOException {
        writer.commitAndGet();
        writer.close();
        writer = null;
        writeBuffer = null;
    }

    public Path getPath() {
        return path;
    }

    public UnsafeSorterHdfsSpillReader getReader(SerializerManager serializerManager) throws IOException {
        return new UnsafeSorterHdfsSpillReader(serializerManager, path, blockId, hadoopConf);
    }

    public int recordsSpilled() {
        return numRecordsSpilled;
    }
}
