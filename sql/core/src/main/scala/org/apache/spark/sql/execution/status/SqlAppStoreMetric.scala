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

import scala.collection.mutable.ListBuffer

import org.apache.spark.status.api.v1.StageStatus

class ApplicationSQLExecutionData(var appId: String = "",
    var appName: String = "",
    var attemptId: String = "",
    var cluster: String = "",
    var queue: String = "",
    var diagnosis: String = "",
    var startTime: Long = 0,
    var endTime: Long = 0,
    var currentTime: Long = 0,
    var host: String = "",
    var service: String = "spark",
    var status: String = "",
    var user: String = "",
    var role: String = "SSL",
    var sqlExecutionData: Option[ExecutionData] = Some(new ExecutionData()),
    var sqlExecutionDatas: Option[ListBuffer[ExecutionData]] = Some(ListBuffer.empty[ExecutionData]),
    var sparkVersion: String = "spark3.1",
    var tarceId: String = "")

class ExecutionData(var statement: String = "",
    var startTimestamp: Long = 0L,
    var userName: String = "") {
  var finishTimestamp: Long = 0L
  var executePlan: String = ""
  var detail: String = ""
  var state: String = ""
  var jobId: ListBuffer[String] = ListBuffer[String]()
  var groupId: String = ""
  var finishAndReport: Boolean = false
  var jobs: ListBuffer[SQLJobData] = ListBuffer.empty
  var duration: Long = 0
  var executionId: Long = -1

  def totalTime: Long = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}


class SQLJobData(
    val jobId: Int,
    val jobGroup: String,
    val describe: String,
    val startTime: Long = 0,
    val endTime: Long = 0,
    val status: String,
    val stageIds: Seq[Int],
    val numTasks: Int,
    val numActiveTasks: Int = 0,
    val numCompletedTasks: Int = 0,
    val numSkippedTasks: Int = 0,
    val numFailedTasks: Int = 0,
    val numKilledTasks: Int = 0,
    val numCompletedIndices: Int = 0,
    val numActiveStages: Int = 0,
    val numCompletedStages: Int = 0,
    val numSkippedStages: Int = 0,
    val numFailedStages: Int = 0,
    var stages: Seq[SQLStageData] = Seq.empty)

class SQLStageData(
    val status: StageStatus = StageStatus.FAILED,
    val stageId: Int = 0,
    val attemptId: Int = 0,
    val numTasks: Int = 0,
    val numActiveTasks: Int = 0,
    val numCompleteTasks: Int = 0,
    val numFailedTasks: Int = 0,
    val numKilledTasks: Int = 0,
    val numCompletedIndices: Int = 0,
    val executorRunTime: Long = 0,
    val executorCpuTime: Long = 0,
    val submissionTime: Long = 0,
    val firstTaskLaunchedTime: Long = 0,
    val completionTime: Long = 0,
    val failureReason: Option[String] = None,
    val inputBytes: Long = 0,
    val inputRecords: Long = 0,
    val outputBytes: Long = 0,
    val outputRecords: Long = 0,
    val shuffleReadBytes: Long = 0,
    val shuffleReadRecords: Long = 0,
    val shuffleWriteBytes: Long = 0,
    val shuffleWriteRecords: Long = 0,
    val name: String = "",
    var description: Option[String] = None,
    var details: String = "",
    var taskProcessLocalRate: Double = 0,
    var taskNoPrefRate: Double = 0,
    var taskRackLocalRate: Double = 0,
    var taskAnyRate: Double = 0,
    val taskAvgMetrics: Option[SQLTaskAvgMetrics] = None,
    val taskMaxMetrics: Option[SQLTaskMaxMetrics] = None,
    val skewKeys: Seq[String] = Seq.empty,
    var inComing: Int = -1,
    var outGoing: Int = -1)

class SQLTaskAvgMetrics(
    val executorDeserializeTime: Double,
    val executorDeserializeCpuTime: Double,
    val executorRunTime: Double,
    val executorCpuTime: Double,
    val resultSize: Double,
    val jvmGcTime: Double,
    val resultSerializationTime: Double,
    val memoryBytesSpilled: Double,
    val diskBytesSpilled: Double,
    val peakExecutionMemory: Double,
    val inputBytesRead: Double,
    val inputRecordsRead: Double,
    val outputBytesWritten: Double,
    val outputRecordsWritten: Double,
    val shuffleBytesWritten: Double,
    val shuffleWriteTime: Double,
    val shuffleRecordsWritten: Double,
    val shuffleRemoteBlocksFetched: Double,
    val shuffleLocalBlocksFetched: Double,
    val shuffleFetchWaitTime: Double,
    val shuffleRemoteBytesRead: Double,
    val shuffleRemoteBytesReadToDisk: Double,
    val shuffleLocalBytesRead: Double,
    val shuffleRecordsRead: Double,
    val duration: Double,
    val schedulerDelay: Double,
    val avgShuffleReadBytes: Double)

class SQLTaskMaxMetrics(
    val executorDeserializeTime: Double,
    val executorDeserializeCpuTime: Double,
    val executorRunTime: Double,
    val executorCpuTime: Double,
    val resultSize: Double,
    val jvmGcTime: Double,
    val resultSerializationTime: Double,
    val memoryBytesSpilled: Double,
    val diskBytesSpilled: Double,
    val peakExecutionMemory: Double,
    val inputBytesRead: Double,
    val inputRecordsRead: Double,
    val outputBytesWritten: Double,
    val outputRecordsWritten: Double,
    val shuffleBytesWritten: Double,
    val shuffleWriteTime: Double,
    val shuffleRecordsWritten: Double,
    val shuffleRemoteBlocksFetched: Double,
    val shuffleLocalBlocksFetched: Double,
    val shuffleFetchWaitTime: Double,
    val shuffleRemoteBytesRead: Double,
    val shuffleRemoteBytesReadToDisk: Double,
    val shuffleLocalBytesRead: Double,
    val shuffleRecordsRead: Double,
    val duration: Double,
    val schedulerDelay: Double,
    val maxShuffleReadBytes: Double)


