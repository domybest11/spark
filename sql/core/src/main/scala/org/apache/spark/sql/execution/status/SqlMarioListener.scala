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
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ExceptionFailure, JobExecutionStatus, SPARK_VERSION, SparkContext, Success, TaskCommitDenied, TaskEndReason, TaskFailedReason, TaskKilled}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.PUSH_BASED_SHUFFLE_ENABLED
import org.apache.spark.scheduler.{JobFailed, JobSucceeded, SparkListener, SparkListenerBlockManagerAdded, SparkListenerBlockManagerRemoved, SparkListenerBlockUpdated, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerExecutorAdded, SparkListenerExecutorExcluded, SparkListenerExecutorExcludedForStage, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerExecutorReportInfo, SparkListenerExecutorUnexcluded, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerNodeExcluded, SparkListenerNodeExcludedForStage, SparkListenerNodeUnexcluded, SparkListenerResourceProfileAdded, SparkListenerSpeculativeTaskSubmitted, SparkListenerStageCompleted, SparkListenerStageExecutorMetrics, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, SparkListenerUnpersistRDD, SparkListenerUnschedulableTaskSetAdded, SparkListenerUnschedulableTaskSetRemoved, StageInfo}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.status.api.v1
import org.apache.spark.util.{AccumulatorContext, ExceptionRecord, ExecutionDataRecord, JobDataRecord, KafkaProducerUtil, StageDataRecord, TaskAvgMetrics, TaskMaxMetrics, Utils}
import org.apache.spark.util.ExceptionType.SHUFFLE_FAIL

import scala.collection.mutable.HashMap


class SqlMarioListener(sc: SparkContext,
                       kvstore: SqlAppStoreStatusStoreV1,
                       live: Boolean) extends SparkListener with Logging {
  val TRACE_ID_KEY = "spark.trace.id"
  val attemptId: String = sc.applicationAttemptId.getOrElse("0")
  var appId: String = sc.conf.getAppId
  var appName: String = sc.conf.get("spark.app.name", "")
  var submitHost: String = sc.conf.get("spark.submit.host", "")
  var queue: String = sc.conf.get("spark.yarn.queue", "")
  var driverHost: String = sc.conf.get("spark.driver.host", "")
  var sparkVersion: String = SPARK_VERSION
  var user: String = Utils.getCurrentUserName()
  var traceId: String = sc.conf.get(TRACE_ID_KEY, "")

  val numFilesRead = Some("number of files read")
  val numPartitionsRead = Some("number of partitions read")
  val filesSizeRead = Some("size of files read")
  val numFilesWrite = Some("number of written files")
  val filesSizeWrite = Some("written output")
  val numPart = Some("number of dynamic part")
  val mergeNumFiles = Some("merge written files")
  val mergeNumOutputBytes = Some("merge written size")
  val mergeNumOutputRows = Some("merge of output rows")

  private val sqlStages = new ConcurrentHashMap[(Int, Int), SqlStage]()
  private val sqlJobs = new HashMap[Int, SqlJob]()
  private val sqlTasks = new HashMap[Long, SqlTask]()
  private val sqlExecutions = new HashMap[Long, SqlExecution]()
  private val sqlMetrics = new HashMap[(Long, Option[String]), Long]()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stage = getOrCreateStage(stageSubmitted.stageInfo)
    stage.status = v1.StageStatus.ACTIVE

    stage.jobs = sqlJobs.values
      .filter(_.stageIds.contains(stageSubmitted.stageInfo.stageId))
      .toSeq
    stage.jobIds = stage.jobs.map(_.jobId).toSet

    stage.description = Option(stageSubmitted.properties).flatMap { p =>
      Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }
    stage.jobs.foreach { job =>
      job.completedStages = job.completedStages - stageSubmitted.stageInfo.stageId
      job.activeStages += 1
    }

    KafkaProducerUtil.report(new StageDataRecord(
      appName,
      appId,
      stageSubmitted.stageInfo.attemptNumber,
      traceId,
      stage.jobIds,
      stageSubmitted.stageInfo.stageId,
      stageSubmitted.stageInfo.name,
      stage.status.name(),
      stageSubmitted.stageInfo.numTasks,
      submissionTime = stageSubmitted.stageInfo.submissionTime.getOrElse(0),
      isPushBasedShuffleEnabled = sc.conf.get(PUSH_BASED_SHUFFLE_ENABLED),
      details = stageSubmitted.stageInfo.details)
    )

  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val maybeStage = Option(sqlStages.get(
      (stageCompleted.stageInfo.stageId, stageCompleted.stageInfo.attemptNumber)))
    maybeStage.foreach { stage =>
      stage.info = stageCompleted.stageInfo

      stage.status = stageCompleted.stageInfo.failureReason match {
        case Some(_) => v1.StageStatus.FAILED
        case _ if stageCompleted.stageInfo.submissionTime.isDefined => v1.StageStatus.COMPLETE
        case _ => v1.StageStatus.SKIPPED
      }

      stage.jobs.foreach { job =>
        stage.status match {
          case v1.StageStatus.COMPLETE =>
            job.completedStages += stageCompleted.stageInfo.stageId
          case v1.StageStatus.SKIPPED =>
            job.skippedStages += stageCompleted.stageInfo.stageId
            job.skippedTasks += stageCompleted.stageInfo.numTasks
          case _ =>
            job.failedStages += 1
        }
        job.activeStages -= 1
      }
      val removeStage = stage.activeTasks == 0

      val stageDatas = sc.statusStore.stageData(stageCompleted.stageInfo.stageId)
      stageDatas.foreach(stageData => {
        val summary = kvstore.buildSQLStageData(stageData).get
        var taskAvgMetrics: TaskAvgMetrics = null
        var taskMaxMetrics: TaskMaxMetrics = null
        if (summary.taskAvgMetrics.isDefined) {
          val avgMetrics = summary.taskAvgMetrics.get
          val tmaxMetrics = summary.taskMaxMetrics.get
          taskAvgMetrics = new TaskAvgMetrics(
            avgMetrics.executorDeserializeTime, avgMetrics.executorDeserializeCpuTime,
            avgMetrics.executorRunTime, avgMetrics.executorCpuTime,
            avgMetrics.resultSize, avgMetrics.jvmGcTime,
            avgMetrics.resultSerializationTime, avgMetrics.memoryBytesSpilled,
            avgMetrics.diskBytesSpilled, avgMetrics.peakExecutionMemory,
            avgMetrics.inputBytesRead, avgMetrics.inputRecordsRead,
            avgMetrics.outputBytesWritten, avgMetrics.outputRecordsWritten,
            avgMetrics.shuffleBytesWritten, avgMetrics.shuffleWriteTime,
            avgMetrics.shuffleRecordsWritten, avgMetrics.shuffleRemoteBlocksFetched,
            avgMetrics.shuffleLocalBlocksFetched, avgMetrics.shuffleFetchWaitTime,
            avgMetrics.shuffleRemoteBytesRead, avgMetrics.shuffleRemoteBytesReadToDisk,
            avgMetrics.shuffleLocalBytesRead, avgMetrics.shuffleRecordsRead,
            avgMetrics.duration, avgMetrics.schedulerDelay, avgMetrics.avgShuffleReadBytes)

          taskMaxMetrics = new TaskMaxMetrics(
            tmaxMetrics.executorDeserializeTime, tmaxMetrics.executorDeserializeCpuTime,
            tmaxMetrics.executorRunTime, tmaxMetrics.executorCpuTime,
            tmaxMetrics.resultSize, tmaxMetrics.jvmGcTime,
            tmaxMetrics.resultSerializationTime, tmaxMetrics.memoryBytesSpilled,
            tmaxMetrics.diskBytesSpilled, tmaxMetrics.peakExecutionMemory,
            tmaxMetrics.inputBytesRead, tmaxMetrics.inputRecordsRead,
            tmaxMetrics.outputBytesWritten, tmaxMetrics.outputRecordsWritten,
            tmaxMetrics.shuffleBytesWritten, tmaxMetrics.shuffleWriteTime,
            tmaxMetrics.shuffleRecordsWritten, tmaxMetrics.shuffleRemoteBlocksFetched,
            tmaxMetrics.shuffleLocalBlocksFetched,
            tmaxMetrics.shuffleFetchWaitTime, tmaxMetrics.shuffleRemoteBytesRead,
            tmaxMetrics.shuffleRemoteBytesReadToDisk, tmaxMetrics.shuffleLocalBytesRead,
            tmaxMetrics.shuffleRecordsRead, tmaxMetrics.duration,
            tmaxMetrics.schedulerDelay, tmaxMetrics.maxShuffleReadBytes
          )
        }

        KafkaProducerUtil.report(new StageDataRecord(
            appName,
            appId,
            summary.attemptId,
            traceId,
            stage.jobIds,
            summary.stageId,
            summary.name,
            summary.status.name(),
            summary.numTasks,
            summary.numCompleteTasks,
            summary.numFailedTasks,
            summary.numKilledTasks,
            summary.numCompletedIndices,
            summary.firstTaskLaunchedTime,
            summary.submissionTime,
            summary.completionTime,
            summary.failureReason.getOrElse(""),
            stageData.executorDeserializeTime,
            stageData.executorDeserializeCpuTime,
            stageData.resultSize,
            stageData.jvmGcTime,
            stageData.resultSerializationTime,
            stageData.memoryBytesSpilled,
            stageData.diskBytesSpilled,
            stageData.peakExecutionMemory,
            stageData.inputBytes,
            stageData.inputRecords,
            stageData.outputBytes,
            stageData.outputRecords,
            stageData.shuffleReadBytes,
            stageData.executorRunTime,
            stageData.executorCpuTime,
            stageData.shuffleRemoteBlocksFetched,
            stageData.shuffleLocalBlocksFetched,
            stageData.shuffleFetchWaitTime,
            stageData.shuffleRemoteBytesRead,
            stageData.shuffleRemoteBytesReadToDisk,
            stageData.shuffleLocalBytesRead,
            stageData.shuffleReadRecords,
            stageData.shuffleWriteBytes,
            stageData.shuffleWriteTime,
            stageData.shuffleWriteRecords,
            stageData.isPushBasedShuffleEnabled,
            stageData.shuffleCorruptMergedBlockChunks,
            stageData.shuffleFallbackCount,
            stageData.shuffleMergedRemoteBlocksFetched,
            stageData.shuffleMergedLocalBlocksFetched,
            stageData.shuffleMergedRemoteChunksFetched,
            stageData.shuffleMergedLocalChunksFetched,
            stageData.shuffleMergedRemoteBytesRead,
            stageData.shuffleMergedLocalBytesRead,
            stageData.shuffleRemoteReqsDuration,
            stageData.shuffleMergedRemoteReqsDuration,
            stageData.shuffleBlocksPushed,
            stageData.shuffleBlocksNotPushed,
            stageData.shuffleBlocksCollided,
            stageData.shuffleBlocksTooLate,
            stageData.shuffleMergersCount,
            summary.taskAnyRate,
            summary.taskNoPrefRate,
            summary.taskProcessLocalRate,
            summary.taskRackLocalRate,
            Some(taskAvgMetrics),
            Some(taskMaxMetrics),
            summary.skewKeys,
            stageData.details,
            stageData.description.getOrElse(""))
        )

      })
      if (removeStage) {
        sqlStages.remove((stageCompleted.stageInfo.stageId, stageCompleted.stageInfo.attemptNumber))
      }
    }

  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val task = new SqlTask(taskStart.taskInfo, taskStart.stageId, taskStart.stageAttemptId)
    sqlTasks.put(taskStart.taskInfo.taskId, task)

    Option(sqlStages.get((taskStart.stageId, taskStart.stageAttemptId))).foreach { stage =>
      stage.activeTasks += 1

      stage.jobs.foreach { job =>
        job.activeTasks += 1
      }
    }

  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.taskInfo == null) {
      return
    }

    sqlTasks.remove(taskEnd.taskInfo.taskId).foreach { task =>
      task.info = taskEnd.taskInfo

      val errorMessage = taskEnd.reason match {
        case Success =>
          None
        case k: TaskKilled =>
          Some(k.reason)
        case e: ExceptionFailure =>
          Some(e.toErrorString)
        case e: TaskFailedReason =>
          Some(e.toErrorString)
        case other =>
          logInfo(s"Unhandled task end reason: $other")
          None
      }
      task.errorMessage = errorMessage
      task.updateMetrics(taskEnd.taskMetrics)
    }

    val (completedDelta, failedDelta, killedDelta) = taskEnd.reason match {
      case Success =>
        (1, 0, 0)
      case _: TaskKilled =>
        (0, 0, 1)
      case _: TaskCommitDenied =>
        (0, 0, 1)
      case _ =>
        (0, 1, 0)
    }

    Option(sqlStages.get((taskEnd.stageId, taskEnd.stageAttemptId))).foreach { stage =>
      stage.activeTasks -= 1
      stage.completedTasks += completedDelta
      if (completedDelta > 0) {
        stage.completedIndices.add(taskEnd.taskInfo.index)
      }
      stage.failedTasks += failedDelta
      stage.killedTasks += killedDelta
      if (killedDelta > 0) {
        stage.killedSummary = killedTasksSummary(taskEnd.reason, stage.killedSummary)
      }
      val removeStage =
        stage.activeTasks == 0 &&
          (v1.StageStatus.COMPLETE.equals(stage.status) ||
            v1.StageStatus.FAILED.equals(stage.status))

      val taskIndex = (taskEnd.stageId.toLong << Integer.SIZE) | taskEnd.taskInfo.index
      stage.jobs.foreach { job =>
        job.activeTasks -= 1
        job.completedTasks += completedDelta
        if (completedDelta > 0) {
          job.completedIndices.add(taskIndex)
        }
        job.failedTasks += failedDelta
        job.killedTasks += killedDelta
        if (killedDelta > 0) {
          job.killedSummary = killedTasksSummary(taskEnd.reason, job.killedSummary)
        }
      }

      if (removeStage) {
        sqlStages.remove((taskEnd.stageId, taskEnd.stageAttemptId))
      }
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val now = System.nanoTime()
    val jobId = jobStart.jobId
    val stageIds = jobStart.stageIds
    val startTime = jobStart.time
    val status = "RUNNING"
    val numTasks = {
      val missingStages = jobStart.stageInfos.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }

    val description = Option(jobStart.properties)
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)) }
    val jobGroup = Option(jobStart.properties)
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_GROUP_ID)) }
    val sqlExecutionId = Option(jobStart.properties)
      .flatMap(p => Option(p.getProperty("spark.sql.execution.id")).map(_.toLong))
    val job = new SqlJob(
      jobId,
      description,
      if (startTime > 0) Some(new Date(startTime)) else Some(new Date(now)),
      stageIds,
      jobGroup,
      numTasks,
      sqlExecutionId)

    sqlJobs.put(jobStart.jobId, job)

    jobStart.stageInfos.foreach { stageInfo =>
      val stage = getOrCreateStage(stageInfo)
      stage.jobs :+= job
      stage.jobIds += jobStart.jobId
    }
    KafkaProducerUtil.report(new JobDataRecord(
      appId,
      appName,
      attemptId,
      traceId,
      sqlExecutionId.getOrElse(0),
      jobId,
      jobGroup.getOrElse(""),
      description.getOrElse(""),
      startTime = startTime,
      status = status,
      numTasks = numTasks,
      numStages = stageIds.size
    ))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val now = System.nanoTime()
    sqlJobs.remove(jobEnd.jobId).foreach { job =>
      val it = sqlStages.entrySet.iterator()
      while (it.hasNext()) {
        val e = it.next()
        if (job.stageIds.contains(e.getKey()._1)) {
          val stage = e.getValue()
          if (v1.StageStatus.PENDING.equals(stage.status)) {
            stage.status = v1.StageStatus.SKIPPED
            job.skippedStages += stage.info.stageId
            job.skippedTasks += stage.info.numTasks
            job.activeStages -= 1
            it.remove()
          }
        }
      }

      job.status = jobEnd.jobResult match {
        case JobSucceeded =>
          JobExecutionStatus.SUCCEEDED
        case JobFailed(_) =>
          JobExecutionStatus.FAILED
      }

      job.completionTime = if (jobEnd.time > 0) Some(new Date(jobEnd.time)) else Some(new Date(now))

      KafkaProducerUtil.report(new JobDataRecord(
        appId,
        appName,
        attemptId,
        traceId,
        job.sqlExecutionId.getOrElse(0),
        jobEnd.jobId,
        job.jobGroup.getOrElse(""),
        job.description.getOrElse(""),
        job.startTime.get.getTime,
        job.completionTime.get.getTime,
        job.status.name(),
        job.numTasks,
        job.completedTasks,
        job.failedTasks,
        job.skippedTasks,
        job.killedTasks,
        job.completedStages.size + job.skippedStages.size + job.failedStages,
        job.completedStages.size,
        job.skippedStages.size,
        job.failedStages
      ))
    }
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {

  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {

  }

  override def onBlockManagerRemoved(
    blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {

  }

  override def onExecutorMetricsUpdate(
    executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {

  }

  override def onExecutorInfoUpdate(event: SparkListenerExecutorReportInfo): Unit = {
    event.executorReportInfo.exception.foreach {
      case fetchFailedException: FetchFailedException =>
        KafkaProducerUtil.report(new ExceptionRecord(
          appId,
          attemptId,
          traceId,
          event.execId,
          user,
          SHUFFLE_FAIL,
          fetchFailedException
        ))
      case _ =>
    }
  }

  override def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit = {

  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {

  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {

  }

  override def onExecutorExcluded(executorExcluded: SparkListenerExecutorExcluded): Unit = {

  }

  override def onExecutorExcludedForStage(
     executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit = {
  }

  override def onNodeExcludedForStage(
    nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit = {
  }


  override def onExecutorUnexcluded(executorUnexcluded: SparkListenerExecutorUnexcluded): Unit = {

  }

  override def onNodeExcluded(nodeExcluded: SparkListenerNodeExcluded): Unit = {

  }


  override def onNodeUnexcluded(nodeUnexcluded: SparkListenerNodeUnexcluded): Unit = {

  }

  override def onUnschedulableTaskSetAdded(
     unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = {
  }

  override def onUnschedulableTaskSetRemoved(
    unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = {
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
  }

  override def onSpeculativeTaskSubmitted(
    speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case e: SparkListenerDriverAccumUpdates => onDriverAccumUpdates(e)
    case _ => // Ignore
  }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {

  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val SparkListenerSQLExecutionStart(executionId, description, details,
    physicalPlanDescription, sparkPlanInfo, time, properties, sqlText, userName) = event
    val now = System.nanoTime()
    val newUser = if (userName != null) userName else user
    logInfo("userName: " + newUser)
    val startTime = if (time > 0) Some(new Date(time)) else Some(new Date(now))
    val sqlExecution = new SqlExecution(executionId, description, details,
      physicalPlanDescription, sparkPlanInfo, startTime, sqlText)
    sqlExecutions.put(executionId, sqlExecution)
    KafkaProducerUtil.report(new ExecutionDataRecord(
      appId,
      appName,
      attemptId,
      traceId,
      newUser,
      executionId,
      "RUNNING",
      sqlText,
      details,
      physicalPlanDescription,
      time,
      0L)
    )
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val now = System.nanoTime()
    val endTime = if (event.time > 0) event.time else now
    sqlExecutions.remove(event.executionId).foreach { execution =>
      KafkaProducerUtil.report(new ExecutionDataRecord(
        appId,
        appName,
        attemptId,
        traceId,
        "",
        execution.executionId,
        "COMPLETE",
        execution.sqlText,
        execution.details,
        execution.physicalPlanDescription,
        execution.startTime.get.getTime,
        endTime,
        numFilesRead = sqlMetrics.getOrElse((event.executionId, numFilesRead), 0),
        numPartitionsRead = sqlMetrics.getOrElse((event.executionId, numPartitionsRead), 0),
        filesSizeRead = sqlMetrics.getOrElse((event.executionId, filesSizeRead), 0),
        numFilesWrite = sqlMetrics.getOrElse((event.executionId, numFilesWrite), 0),
        filesSizeWrite = sqlMetrics.getOrElse((event.executionId, filesSizeWrite), 0),
        dynamicPart = sqlMetrics.getOrElse((event.executionId, numPart), 0),
        mergeNumFiles = sqlMetrics.getOrElse((event.executionId, mergeNumFiles), 0),
        mergeNumOutputBytes = sqlMetrics.getOrElse((event.executionId, mergeNumOutputBytes), 0),
        mergeNumOutputRows = sqlMetrics.getOrElse((event.executionId, mergeNumOutputRows), 0)
      ))
    }
    sqlMetrics.foreach(sqlMetric => {
      if (sqlMetric._1._1.equals(event.executionId)) {
        sqlMetrics.remove(event.executionId, sqlMetric._1._2)
      }
    })
  }

  private def onDriverAccumUpdates(event: SparkListenerDriverAccumUpdates): Unit = {
    event.accumUpdates.foreach(accumUpdate => {
      val accumulator = AccumulatorContext.get(accumUpdate._1)
      if (accumulator.isDefined) {
        sqlMetrics.put((event.executionId, accumulator.get.name), accumUpdate._2)
      }
    })
  }

  private def getOrCreateStage(info: StageInfo): SqlStage = {
    val stage = sqlStages.computeIfAbsent((info.stageId, info.attemptNumber),
      (_: (Int, Int)) => new SqlStage())
    stage.info = info
    stage
  }

  private def killedTasksSummary(
                                  reason: TaskEndReason,
                                  oldSummary: Map[String, Int]): Map[String, Int] = {
    reason match {
      case k: TaskKilled =>
        oldSummary.updated(k.reason, oldSummary.getOrElse(k.reason, 0) + 1)
      case denied: TaskCommitDenied =>
        val reason = denied.toErrorString
        oldSummary.updated(reason, oldSummary.getOrElse(reason, 0) + 1)
      case _ =>
        oldSummary
    }
  }
}
