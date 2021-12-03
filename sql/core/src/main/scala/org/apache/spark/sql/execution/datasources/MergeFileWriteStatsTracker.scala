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

package org.apache.spark.sql.execution.datasources

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.SerializableConfiguration


case class MergeFileWriteTaskStats(
    numFiles: Int,
    numBytes: Long,
    numRows: Long)
  extends WriteTaskStats

class MergeFileWriteTaskStatsTracker(hadoopConf: Configuration)
  extends WriteTaskStatsTracker with Logging {

  private[this] var numFiles: Int = 0
  private[this] var submittedFiles: Int = 0
  private[this] var numBytes: Long = 0L
  private[this] var numRows: Long = 0L
  private[this] var curFile: Option[String] = None

  /**
   * Get the size of the file expected to have been written by a worker.
   * @param filePath path to the file
   * @return the file size or None if the file was not found.
   */
  private def getFileSize(filePath: String): Option[Long] = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    try {
      Some(fs.getFileStatus(path).getLen())
    } catch {
      case e: FileNotFoundException =>
        // may arise against eventually consistent object stores
        logDebug(s"File $path is not yet visible", e)
        None
    }
  }

  override def newPartition(partitionValues: InternalRow): Unit = {
  }

  override def newBucket(bucketId: Int): Unit = {
    // currently unhandled
  }

  override def newFile(filePath: String): Unit = {
    statCurrentFile()
    curFile = Some(filePath)
    submittedFiles += 1
  }

  private def statCurrentFile(): Unit = {
    curFile.foreach { path =>
      getFileSize(path).foreach { len =>
        numBytes += len
        numFiles += 1
      }
      curFile = None
    }
  }

  override def newRow(row: InternalRow): Unit = {
    numRows += 1
  }

  override def getFinalStats(): WriteTaskStats = {
    statCurrentFile()

    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach { outputMetrics =>
      outputMetrics.setBytesWritten(numBytes)
      outputMetrics.setRecordsWritten(numRows)
    }

    if (submittedFiles != numFiles) {
      logInfo(s"Expected $submittedFiles files, but only saw $numFiles. " +
        "This could be due to the output format not writing empty files, " +
        "or files being not immediately visible in the filesystem.")
    }
    MergeFileWriteTaskStats(numFiles, numBytes, numRows)
  }
}

class MergeWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient val metrics: Map[String, SQLMetric])
  extends WriteJobStatsTracker {

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new MergeFileWriteTaskStatsTracker(serializableHadoopConf.value)
  }

  override def processStats(stats: Seq[WriteTaskStats]): Unit = {
    val sparkContext = SparkContext.getActive.get
    var numFiles: Long = 0L
    var totalNumBytes: Long = 0L
    var totalNumOutput: Long = 0L

    val basicStats = stats.map(_.asInstanceOf[MergeFileWriteTaskStats])

    basicStats.foreach { summary =>
      numFiles += summary.numFiles
      totalNumBytes += summary.numBytes
      totalNumOutput += summary.numRows
    }

    metrics(MergeWriteJobStatsTracker.M_NUM_FILES_KEY).add(numFiles)
    metrics(MergeWriteJobStatsTracker.M_NUM_OUTPUT_BYTES_KEY).add(totalNumBytes)
    metrics(MergeWriteJobStatsTracker.M_NUM_OUTPUT_ROWS_KEY).add(totalNumOutput)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toList)
  }
}

object MergeWriteJobStatsTracker {
  private val M_NUM_FILES_KEY = "mergeNumFiles"
  private val M_NUM_OUTPUT_BYTES_KEY = "mergeNumOutputBytes"
  private val M_NUM_OUTPUT_ROWS_KEY = "mergeNumOutputRows"

  def metrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
      M_NUM_FILES_KEY -> SQLMetrics.createMetric(sparkContext, "merge written files"),
      M_NUM_OUTPUT_BYTES_KEY -> SQLMetrics.createSizeMetric(sparkContext, "merge written size"),
      M_NUM_OUTPUT_ROWS_KEY -> SQLMetrics.createMetric(sparkContext, "merge of output rows"),
    )
  }
}
