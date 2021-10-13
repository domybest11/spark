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

package org.apache.spark.sql.execution.status

import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{StageInfo, TaskInfo}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.status.LiveEntityHelpers.{createMetrics, subtractMetrics}
import org.apache.spark.status.api.v1
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.collection.OpenHashSet

private[spark] abstract class SqlEntity {

}

private class SqlExecution(
   val executionId: Long,
   val description: String,
   val details: String,
   val physicalPlanDescription: String,
   val sparkPlanInfo: SparkPlanInfo,
   val startTime: Option[Date],
   var sqlText: String = null) extends SqlEntity {
}

private class SqlJob(
    val jobId: Int,
    val description: Option[String],
    val startTime: Option[Date],
    val stageIds: Seq[Int],
    val jobGroup: Option[String],
    val numTasks: Int,
    val sqlExecutionId: Option[Long]) extends SqlEntity {

  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0

  val completedIndices = new OpenHashSet[Long]()

  var killedTasks = 0
  var killedSummary: Map[String, Int] = Map()

  var skippedTasks = 0
  var skippedStages = Set[Int]()

  var status = JobExecutionStatus.RUNNING
  var completionTime: Option[Date] = None

  var completedStages: Set[Int] = Set()
  var activeStages = 0
  var failedStages = 0
}

private class SqlStage extends SqlEntity {

  var jobs = Seq[SqlJob]()
  var jobIds = Set[Int]()

  var info: StageInfo = null
  var status = v1.StageStatus.PENDING

  var description: Option[String] = None
  var schedulingPool: String = SparkUI.DEFAULT_POOL_NAME

  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0
  val completedIndices = new OpenHashSet[Int]()

  var killedTasks = 0
  var killedSummary: Map[String, Int] = Map()

  @volatile var cleaning = false
  var savedTasks = new AtomicInteger(0)

}

private class SqlTask(
  var info: TaskInfo,
  stageId: Int,
  stageAttemptId: Int) extends SqlEntity {

  private var metrics: v1.TaskMetrics = createMetrics(default = -1L)

  var errorMessage: Option[String] = None

  def updateMetrics(metrics: TaskMetrics): v1.TaskMetrics = {
    if (metrics != null) {
      val old = this.metrics
      val newMetrics = createMetrics(
        metrics.executorDeserializeTime,
        metrics.executorDeserializeCpuTime,
        metrics.executorRunTime,
        metrics.executorCpuTime,
        metrics.resultSize,
        metrics.jvmGCTime,
        metrics.resultSerializationTime,
        metrics.memoryBytesSpilled,
        metrics.diskBytesSpilled,
        metrics.peakExecutionMemory,
        metrics.inputMetrics.bytesRead,
        metrics.inputMetrics.recordsRead,
        metrics.outputMetrics.bytesWritten,
        metrics.outputMetrics.recordsWritten,
        metrics.shuffleReadMetrics.remoteBlocksFetched,
        metrics.shuffleReadMetrics.localBlocksFetched,
        metrics.shuffleReadMetrics.fetchWaitTime,
        metrics.shuffleReadMetrics.remoteBytesRead,
        metrics.shuffleReadMetrics.remoteBytesReadToDisk,
        metrics.shuffleReadMetrics.localBytesRead,
        metrics.shuffleReadMetrics.recordsRead,
        metrics.shuffleReadMetrics.corruptMergedBlockChunks,
        metrics.shuffleReadMetrics.fallbackCount,
        metrics.shuffleReadMetrics.remoteMergedBlocksFetched,
        metrics.shuffleReadMetrics.localMergedBlocksFetched,
        metrics.shuffleReadMetrics.remoteMergedChunksFetched,
        metrics.shuffleReadMetrics.localMergedChunksFetched,
        metrics.shuffleReadMetrics.remoteMergedBlocksBytesRead,
        metrics.shuffleReadMetrics.localMergedBlocksBytesRead,
        metrics.shuffleReadMetrics.remoteReqsDuration,
        metrics.shuffleReadMetrics.remoteMergedReqsDuration,
        metrics.shuffleWriteMetrics.bytesWritten,
        metrics.shuffleWriteMetrics.writeTime,
        metrics.shuffleWriteMetrics.recordsWritten,
        metrics.shuffleWriteMetrics.blocksPushed,
        metrics.shuffleWriteMetrics.blocksNotPushed,
        metrics.shuffleWriteMetrics.blocksCollided,
        metrics.shuffleWriteMetrics.blocksTooLate)

      this.metrics = newMetrics

      if (old.executorDeserializeTime >= 0L) {
        subtractMetrics(newMetrics, old)
      } else {
        newMetrics
      }
    } else {
      null
    }
  }
}
