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
package org.apache.spark.storage

import java.io.{BufferedInputStream, FileInputStream}

import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream}

import org.apache.spark.serializer.{DeserializationStream, SerializerInstance, SerializerManager}

abstract class SpillBasedReader[K, C]() {
  def readNextPartition(): Iterator[Product2[K, C]]
}


/**
 * An internal class for reading a spilled file partition by partition. Expects all the
 * partitions to be requested in order.
 */
 class SpillDiskReader[K, C](spill: SpilledFile,
                         serializerManager: SerializerManager,
                         serInstance: SerializerInstance,
                         numPartitions: Int,
                         serializerBatchSize: Long,
                         hadoopConf: Configuration) extends SpillBasedReader[K, C] {
  // Serializer batch offsets; size will be batchSize.length + 1
  val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

  // Track which partition and which batch stream we're in. These will be the indices of
  // the next element we will read. We'll also store the last partition read so that
  // readNextPartition() can figure out what partition that was from.
  var partitionId = 0
  var indexInPartition = 0L
  var batchId = 0
  var indexInBatch = 0
  var lastPartitionId = 0

  skipToNextPartition()

  // Intermediate file and deserializer streams that read from exactly one batch
  // This guards against pre-fetching and other arbitrary behavior of higher level streams
  var fileStream: FileInputStream = null
  var deserializeStream = nextBatchStream()  // Also sets fileStream

  var nextItem: (K, C) = null
  var finished = false

  /** Construct a stream that only reads from the next batch */
  def nextBatchStream(): DeserializationStream = {
    // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
    // we're still in a valid batch.
    if (batchId < batchOffsets.length - 1) {
      if (deserializeStream != null) {
        deserializeStream.close()
        fileStream.close()
        deserializeStream = null
        fileStream = null
      }

      val start = batchOffsets(batchId)
      fileStream = new FileInputStream(spill.file)
      fileStream.getChannel.position(start)
      batchId += 1

      val end = batchOffsets(batchId)

      assert(end >= start, "start = " + start + ", end = " + end +
        ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

      val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))

      val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
      serInstance.deserializeStream(wrappedStream)
    } else {
      // No more batches left
      cleanup()
      null
    }
  }

  /**
   * Update partitionId if we have reached the end of our current partition, possibly skipping
   * empty partitions on the way.
   */
  private def skipToNextPartition(): Unit = {
    while (partitionId < numPartitions &&
      indexInPartition == spill.elementsPerPartition(partitionId)) {
      partitionId += 1
      indexInPartition = 0L
    }
  }

  /**
   * Return the next (K, C) pair from the deserialization stream and update partitionId,
   * indexInPartition, indexInBatch and such to match its location.
   *
   * If the current batch is drained, construct a stream for the next batch and read from it.
   * If no more pairs are left, return null.
   */
  private def readNextItem(): (K, C) = {
    if (finished || deserializeStream == null) {
      return null
    }
    val k = deserializeStream.readKey().asInstanceOf[K]
    val c = deserializeStream.readValue().asInstanceOf[C]
    lastPartitionId = partitionId
    // Start reading the next batch if we're done with this one
    indexInBatch += 1
    if (indexInBatch == serializerBatchSize) {
      indexInBatch = 0
      deserializeStream = nextBatchStream()
    }
    // Update the partition location of the element we're reading
    indexInPartition += 1
    skipToNextPartition()
    // If we've finished reading the last partition, remember that we're done
    if (partitionId == numPartitions) {
      finished = true
      if (deserializeStream != null) {
        deserializeStream.close()
      }
    }
    (k, c)
  }

  var nextPartitionToRead = 0

  override def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
    val myPartition = nextPartitionToRead
    nextPartitionToRead += 1

    override def hasNext: Boolean = {
      if (nextItem == null) {
        nextItem = readNextItem()
        if (nextItem == null) {
          return false
        }
      }
      assert(lastPartitionId >= myPartition)
      // Check that we're still in the right partition; note that readNextItem will have returned
      // null at EOF above so we would've returned false there
      lastPartitionId == myPartition
    }

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val item = nextItem
      nextItem = null
      item
    }
  }

  // Clean up our open streams and put us in a state where we can't read any more data
  def cleanup(): Unit = {
    batchId = batchOffsets.length  // Prevent reading any other batch
    val ds = deserializeStream
    deserializeStream = null
    fileStream = null
    if (ds != null) {
      ds.close()
    }
    // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
    // This should also be fixed in ExternalAppendOnlyMap.
  }
}


/**
 * An internal class for reading a hdfs spilled file partition by partition. Expects all the
 * partitions to be requested in order.
 */
private[this] class SpillHdfsReader[K, C](spill: SpilledFile,
                                          serializerManager: SerializerManager,
                                          serInstance: SerializerInstance,
                                          numPartitions: Int,
                                          serializerBatchSize: Long,
                                          hadoopConf: Configuration)
  extends SpillBasedReader[K, C] {

  val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)
  var partitionId = 0
  var indexInPartition = 0L
  var batchId = 0
  var indexInBatch = 0
  var lastPartitionId = 0

  skipToNextPartition()

  var fsDataInputStream: FSDataInputStream = null
  var deserializeStream = nextBatchStream()  // Also sets fileStream

  var nextItem: (K, C) = null
  var finished = false

  /** Construct a stream that only reads from the next batch */
  def nextBatchStream(): DeserializationStream = {
    // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
    // we're still in a valid batch.
    if (batchId < batchOffsets.length - 1) {
      if (deserializeStream != null) {
        deserializeStream.close()
        deserializeStream = null
      }

      val start = batchOffsets(batchId)
      val fileSystem: FileSystem =
        ShuffleStorageUtils.getFileSystemForPath(spill.path.get, hadoopConf)
      fsDataInputStream = fileSystem.open(spill.path.get)
      /*   cstream = compressionCodec.map(_.compressedContinuousInputStream(fsDataInputStream))
             .getOrElse(fsDataInputStream) */
      fsDataInputStream.seek(start)
      batchId += 1
      val end = batchOffsets(batchId)

      assert(end >= start, "start = " + start + ", end = " + end +
        ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

      val bufferedStream =
        new BufferedInputStream(ByteStreams.limit(fsDataInputStream, end - start))

      val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
      serInstance.deserializeStream(wrappedStream)
    } else {
      // No more batches left
      cleanup()
      null
    }
  }

  /**
   * Update partitionId if we have reached the end of our current partition, possibly skipping
   * empty partitions on the way.
   */
  private def skipToNextPartition(): Unit = {
    while (partitionId < numPartitions &&
      indexInPartition == spill.elementsPerPartition(partitionId)) {
      partitionId += 1
      indexInPartition = 0L
    }
  }

  /**
   * Return the next (K, C) pair from the deserialization stream and update partitionId,
   * indexInPartition, indexInBatch and such to match its location.
   *
   * If the current batch is drained, construct a stream for the next batch and read from it.
   * If no more pairs are left, return null.
   */
  private def readNextItem(): (K, C) = {
    if (finished || deserializeStream == null) {
      return null
    }
    val k = deserializeStream.readKey().asInstanceOf[K]
    val c = deserializeStream.readValue().asInstanceOf[C]
    lastPartitionId = partitionId
    // Start reading the next batch if we're done with this one
    indexInBatch += 1
    if (indexInBatch == serializerBatchSize) {
      indexInBatch = 0
      deserializeStream = nextBatchStream()
    }
    // Update the partition location of the element we're reading
    indexInPartition += 1
    skipToNextPartition()
    // If we've finished reading the last partition, remember that we're done
    if (partitionId == numPartitions) {
      finished = true
      if (deserializeStream != null) {
        deserializeStream.close()
      }
    }
    (k, c)
  }

  var nextPartitionToRead = 0

  override def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
    val myPartition = nextPartitionToRead
    nextPartitionToRead += 1

    override def hasNext: Boolean = {
      if (nextItem == null) {
        nextItem = readNextItem()
        if (nextItem == null) {
          return false
        }
      }
      assert(lastPartitionId >= myPartition)
      // Check that we're still in the right partition; note that readNextItem will have returned
      // null at EOF above so we would've returned false there
      lastPartitionId == myPartition
    }

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val item = nextItem
      nextItem = null
      item
    }
  }

  // Clean up our open streams and put us in a state where we can't read any more data
  def cleanup(): Unit = {
    batchId = batchOffsets.length  // Prevent reading any other batch
    val ds = deserializeStream
    deserializeStream = null
    if (ds != null) {
      ds.close()
    }
    // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
    // This should also be fixed in ExternalAppendOnlyMap.
  }
}