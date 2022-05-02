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

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.{ClosedByInterruptException, FileChannel}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_COMPRESSION_CODEC
import org.apache.spark.io.{CompressionCodec, MutableCheckedOutputStream}
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.PairsWriter

private[spark] class HdfsBlockObjectWriter(
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    writeMetrics: ShuffleWriteMetricsReporter,
    val blockId: BlockId = null,
    path: Path,
    hadoopConf: Configuration,
    sparkConf: SparkConf)
  extends OutputStream
    with Logging
    with PairsWriter {

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }
  private var fsDataOutputStream: FSDataOutputStream = null
  private var cstream: OutputStream = null
  private val shouldCompress =
    sparkConf.get("spark.spill.data.compress", "false").toBoolean
  private val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf, sparkConf.get(EVENT_LOG_COMPRESSION_CODEC)))
    } else {
      None
    }

  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false
  private var committedPosition: Long = 0

  private var numRecordsWritten = 0

  private def initialize(): Unit = {
    val fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf)
    if (fileSystem.isFile(path)) {
      if (hadoopConf.getBoolean("dfs.support.append", true)) {
        fsDataOutputStream = fileSystem.append(path)
        cstream = compressionCodec.map(_.compressedContinuousOutputStream(fsDataOutputStream))
          .getOrElse(fsDataOutputStream)
        committedPosition = fsDataOutputStream.getPos
      } else {
        val msg = s"$path exists but append mode is not support!"
        logError(msg)
        throw new IllegalStateException(msg)
      }
    } else if (fileSystem.isDirectory(path)) {
      val msg = s"$path is a directory!"
      logError(msg)
      throw new IllegalStateException(msg)
    } else {
      fsDataOutputStream = fileSystem.create(path)
      cstream = compressionCodec.map(_.compressedContinuousOutputStream(fsDataOutputStream))
        .getOrElse(fsDataOutputStream)
      committedPosition = fsDataOutputStream.getPos
    }
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(cstream, bufferSize)
        with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  def open(): HdfsBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }
    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        mcs = null
        bs = null
        ts = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
  }

  /**
   * Commits any remaining partial writes and closes resources.
   */
  override def close(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): FileSegment = {
    if (streamOpen) {
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      val pos = fsDataOutputStream.getPos
      val fileSegment = new FileSegment(null, committedPosition, pos - committedPosition,
        Some(path))
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(pos - committedPosition)
      numRecordsWritten = 0
      fileSegment
    } else {
      new FileSegment(null, committedPosition, 0, Some(path))
    }
  }


  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this HdfsBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    Utils.tryWithSafeFinally {
      if (initialized) {
        // writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
      try {
        val fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf)
        fileSystem.truncate(path, committedPosition)
      } catch {
        case ce: ClosedByInterruptException =>
          logError("Exception occurred while reverting partial writes to file "
            + path.toUri.getPath + ", " + ce.getMessage)
        case e: Exception =>
          logError("Uncaught exception while reverting partial writes to file "
            + path.toUri.getPath, e)
      }
    }
    null
  }

  /**
   * Writes a key-value pair.
   */
  override def write(key: Any, value: Any): Unit = {
    if (!streamOpen) {
      open()
    }
    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)
  }



  // For testing
  private[spark] override def flush(): Unit = {
    objOut.flush()
    bs.flush()
  }
}
