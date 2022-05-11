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

import java.io.{BufferedInputStream, EOFException, File, FileInputStream}

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializerInstance, SerializerManager}


// Information about a spilled file. Includes sizes in bytes of "batches" written by the
// serializer as we periodically reset its stream, as well as number of elements in each
// partition, used to efficiently keep track of partitions when merging.
case class SpilledFile(
     file: File,
     blockId: BlockId,
     serializerBatchSizes: Array[Long],
     elementsPerPartition: Array[Long],
     path: Option[Path] = None)

/**
 * An iterator that returns (K, C) pairs in sorted order from an on-disk map
 */
class DiskMapIterator[K, C] (file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long],
                             serializerManager: SerializerManager, ser: SerializerInstance,
                             serializerBatchSize: Long, context: TaskContext)
  extends Iterator[(K, C)] with Logging{
  private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
  assert(file.length() == batchOffsets.last,
    "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
  )

  private var batchIndex = 0  // Which batch we're in
  private var fileStream: FileInputStream = null

  // An intermediate stream that reads from exactly one batch
  // This guards against pre-fetching and other arbitrary behavior of higher level streams
  private var deserializeStream: DeserializationStream = null
  private var nextItem: (K, C) = null
  private var objectsRead = 0

  /**
   * Construct a stream that reads only from the next batch.
   */
  private def nextBatchStream(): DeserializationStream = {
    // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
    // we're still in a valid batch.
    if (batchIndex < batchOffsets.length - 1) {
      if (deserializeStream != null) {
        deserializeStream.close()
        fileStream.close()
        deserializeStream = null
        fileStream = null
      }

      val start = batchOffsets(batchIndex)
      fileStream = new FileInputStream(file)
      fileStream.getChannel.position(start)
      batchIndex += 1

      val end = batchOffsets(batchIndex)

      assert(end >= start, "start = " + start + ", end = " + end +
        ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

      val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
      val wrappedStream = serializerManager.wrapStream(blockId, bufferedStream)
      ser.deserializeStream(wrappedStream)
    } else {
      // No more batches left
      cleanup()
      null
    }
  }

  /**
   * Return the next (K, C) pair from the deserialization stream.
   *
   * If the current batch is drained, construct a stream for the next batch and read from it.
   * If no more pairs are left, return null.
   */
  private def readNextItem(): (K, C) = {
    try {
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      val item = (k, c)
      objectsRead += 1
      if (objectsRead == serializerBatchSize) {
        objectsRead = 0
        deserializeStream = nextBatchStream()
      }
      item
    } catch {
      case e: EOFException =>
        cleanup()
        null
    }
  }

  override def hasNext: Boolean = {
    if (nextItem == null) {
      if (deserializeStream == null) {
        // In case of deserializeStream has not been initialized
        deserializeStream = nextBatchStream()
        if (deserializeStream == null) {
          return false
        }
      }
      nextItem = readNextItem()
    }
    nextItem != null
  }

  override def next(): (K, C) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val item = nextItem
    nextItem = null
    item
  }

  private def cleanup(): Unit = {
    batchIndex = batchOffsets.length  // Prevent reading any other batch
    if (deserializeStream != null) {
      deserializeStream.close()
      deserializeStream = null
    }
    if (fileStream != null) {
      fileStream.close()
      fileStream = null
    }
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting ${file}")
      }
    }
  }

  context.addTaskCompletionListener[Unit](context => cleanup())
}


/**
 * An iterator that returns (K, C) pairs in sorted order from an on-hdfs map
 */
private class HdfsMapIterator[K, C] (path: Path, blockId: BlockId, batchSizes: ArrayBuffer[Long],
                                     serializerManager: SerializerManager, ser: SerializerInstance,
                                     serializerBatchSize: Long, context: TaskContext,
                                     hadoopConf: Configuration)
  extends Iterator[(K, C)] with  Logging
{
  private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)
  private var batchIndex = 0  // Which batch we're in
  var fsDataInputStream: FSDataInputStream = null
  private var deserializeStream: DeserializationStream = null
  private var nextItem: (K, C) = null
  private var objectsRead = 0

  /**
   * Construct a stream that reads only from the next batch.
   */
  private def nextBatchStream(): DeserializationStream = {
    // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
    // we're still in a valid batch.
    if (batchIndex < batchOffsets.length - 1) {
      if (deserializeStream != null) {
        deserializeStream.close()
        deserializeStream = null
      }

      val start = batchOffsets(batchIndex)
      val fileSystem: FileSystem =
        ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf)
      fsDataInputStream = fileSystem.open(path)
      fsDataInputStream.seek(start)
      batchIndex += 1
      val end = batchOffsets(batchIndex)

      assert(end >= start, "start = " + start + ", end = " + end +
        ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

      val bufferedStream =
        new BufferedInputStream(ByteStreams.limit(fsDataInputStream, end - start))
      val wrappedStream = serializerManager.wrapStream(blockId, bufferedStream)
      ser.deserializeStream(wrappedStream)
    } else {
      // No more batches left
      cleanup()
      null
    }
  }

  /**
   * Return the next (K, C) pair from the deserialization stream.
   *
   * If the current batch is drained, construct a stream for the next batch and read from it.
   * If no more pairs are left, return null.
   */
  private def readNextItem(): (K, C) = {
    try {
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      val item = (k, c)
      objectsRead += 1
      if (objectsRead == serializerBatchSize) {
        objectsRead = 0
        deserializeStream = nextBatchStream()
      }
      item
    } catch {
      case e: EOFException =>
        cleanup()
        null
    }
  }

  override def hasNext: Boolean = {
    if (nextItem == null) {
      if (deserializeStream == null) {
        // In case of deserializeStream has not been initialized
        deserializeStream = nextBatchStream()
        if (deserializeStream == null) {
          return false
        }
      }
      nextItem = readNextItem()
    }
    nextItem != null
  }

  override def next(): (K, C) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val item = nextItem
    nextItem = null
    item
  }

  private def cleanup(): Unit = {
    batchIndex = batchOffsets.length  // Prevent reading any other batch
    if (deserializeStream != null) {
      deserializeStream.close()
      deserializeStream = null
    }
    val fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf)
    if (fileSystem.exists(path)) {
      if (!fileSystem.delete(path)) {
        logWarning(s"Error deleting ${path.toUri.getPath}")
      }
    }
  }

  context.addTaskCompletionListener[Unit](context => cleanup())
}