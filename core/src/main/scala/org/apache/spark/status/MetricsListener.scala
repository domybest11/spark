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

package org.apache.spark.status

import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.event.SimpleWrapEvent
import org.apache.spark.metrics.sink.KafkaSink
import org.apache.spark.scheduler._
import org.apache.spark.status.api.v1
import org.apache.spark.storage._

import scala.collection.mutable.HashMap

/**
 * A Spark listener that writes Metrics information to a data store. The types written to the
 * kafka are defined in the `kafkaSink.scala` file.
 *
 * @param lastUpdateTime When replaying logs, the log's last update time, so that the duration of
 *                       unfinished tasks can be more accurately calculated (see SPARK-21922).
 */
private[spark] class MetricsListener(
    conf: SparkConf,
    kafkaSink: KafkaSink,
    live: Boolean,
    lastUpdateTime: Option[Long] = None) extends SparkListener with Logging {

  private var sparkVersion = SPARK_VERSION
  private var appInfo: status.ApplicationMetricData = null

  private var coresPerTask: Int = 1



  // Keep track of live entities, so that task metrics can be efficiently updated (without
  // causing too many writes to the underlying store, and other expensive operations).
  private val liveStages = new ConcurrentHashMap[(Int, Int), LiveMetricStage]()
  private val liveJobs = new HashMap[Int, LiveMetricJob]()
  private val liveExecutors = new HashMap[String, LiveMetricExecutor]()
  private val liveTasks = new HashMap[Long, LiveMetricTask]()

  // Keep the active executor count as a separate variable to avoid having to do synchronization
  // around liveExecutors.
  @volatile private var activeExecutorCount = 0

  /** The last time when flushing `LiveEntity`s. This is to avoid flushing too frequently. */
  private var lastFlushTimeNs = System.nanoTime()


  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerLogStart(version) => sparkVersion = version
    case _ =>
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    assert(event.appId.isDefined, "Application without IDs are not supported.")
    appInfo = status.ApplicationMetricData(
      event.appId.get,
      event.appId.get,
      event.appName,
      new Date(event.time),
      new Date(-1),
      false
    )

    kafkaSink.report(KafkaSink.METRIC_TOPIC,
      new SimpleWrapEvent(MetricEventTypeEnums.APPLICATIONSTART.toString,
      MetricEventNameEnums.APPLICATION.toString, appInfo))
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {

  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    appInfo = status.ApplicationMetricData(
      appInfo.appId,
      appInfo.appId,
      appInfo.appName,
      appInfo.startTime,
      new Date(event.time),
      true
    )

    kafkaSink.report(KafkaSink.METRIC_TOPIC,
      new SimpleWrapEvent(MetricEventTypeEnums.APPLICATIONEND.toString,
      MetricEventNameEnums.APPLICATION.toString, appInfo))
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    // This needs to be an update in case an executor re-registers after the driver has
    // marked it as "dead".
    val exec = getOrCreateExecutor(event.executorId, event.time)

    exec.host = event.executorInfo.executorHost
    exec.isActive = true
    exec.id = event.executorInfo.executorHost + "-" + event.executorId
    exec.totalCores = event.executorInfo.totalCores
    exec.maxTasks = event.executorInfo.totalCores / coresPerTask
    exec.executorLogs = event.executorInfo.logUrlMap
    liveUpdate(exec, MetricEventNameEnums.EXECUTOR.toString,
      MetricEventTypeEnums.EXECUTORSTART.toString)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    liveExecutors.remove(event.executorId).foreach { exec =>
      activeExecutorCount = math.max(0, activeExecutorCount - 1)
      exec.isActive = false
      exec.removeTime = new Date(event.time)
      exec.removeReason = event.reason
      update(exec, MetricEventNameEnums.EXECUTOR.toString, MetricEventTypeEnums.EXECUTOREND.toString)
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val numTasks = {
      val missingStages = event.stageInfos.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }

    val jobGroup = Option(event.properties)
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_GROUP_ID)) }

    val job = new LiveMetricJob(
      appInfo.appId + "-" + event.jobId,
      appInfo.appId,
      event.jobId,
      if (event.time > 0) Some(new Date(event.time)) else None,
      event.stageIds,
      jobGroup,
      numTasks)
    liveJobs.put(event.jobId, job)
    liveUpdate(job, MetricEventNameEnums.JOB.toString, MetricEventTypeEnums.JOBSTART.toString)

    event.stageInfos.foreach { stageInfo =>
      // A new job submission may re-use an existing stage, so this code needs to do an update
      // instead of just a write.
      val stage = getOrCreateStage(stageInfo)
      stage.jobs :+= job
      stage.jobIds += event.jobId
      stage.id = Option(appInfo.appId + "-" + stage.jobIds.addString(new StringBuilder(), "-") + "-" + stageInfo.stageId)
      liveUpdate(stage, MetricEventNameEnums.STAGE.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
    }
  }



  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    liveJobs.remove(event.jobId).foreach { job =>

      val it = liveStages.entrySet.iterator()
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
            liveUpdate(stage, MetricEventNameEnums.STAGE.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
          }
        }
      }

      job.status = event.jobResult match {
        case JobSucceeded => JobExecutionStatus.SUCCEEDED
        case JobFailed(_) => JobExecutionStatus.FAILED
      }

      job.completionTime = if (event.time > 0) Some(new Date(event.time)) else None
      job.complete = true
      liveUpdate(job, MetricEventNameEnums.JOB.toString, MetricEventTypeEnums.JOBEND.toString)
    }
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stage = getOrCreateStage(event.stageInfo)
    stage.status = v1.StageStatus.ACTIVE
//    stage.schedulingPool = Option(event.properties).flatMap { p =>
//      Option(p.getProperty("spark.scheduler.pool"))
//    }.getOrElse(SparkUI.DEFAULT_POOL_NAME)

    // Look at all active jobs to find the ones that mention this stage.
    stage.jobs = liveJobs.values
      .filter(_.stageIds.contains(event.stageInfo.stageId))
      .toSeq
    stage.jobIds = stage.jobs.map(_.jobId).toSet

    stage.id = Option(appInfo.appId + "-" + stage.jobIds.addString(new StringBuilder(), "-") + "-" + event.stageInfo.stageId)

    stage.description = Option(event.properties).flatMap { p =>
      Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }

    stage.jobs.foreach { job =>
      job.completedStages = job.completedStages - event.stageInfo.stageId
      job.activeStages += 1
      liveUpdate(job, MetricEventNameEnums.JOB.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
    }

    liveUpdate(stage, MetricEventNameEnums.STAGE.toString, MetricEventTypeEnums.STAGESTART.toString)
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val jobIds = liveStages.get((event.stageId, event.stageAttemptId)).jobIds

    val id = appInfo.appId + "-" + jobIds.addString(new StringBuilder(), "-") + "-" + event.stageId + "-" +
      event.taskInfo.executorId + "-" + event.taskInfo.taskId

    val task = new LiveMetricTask(id, event.taskInfo, appInfo.appId, jobIds, event.stageId,
      event.taskInfo.executorId, event.stageAttemptId,
      if (event.taskInfo.launchTime > 0) Some(new Date(event.taskInfo.launchTime)) else None, lastUpdateTime)
    liveTasks.put(event.taskInfo.taskId, task)

    liveUpdate(task, MetricEventNameEnums.TASK.toString, MetricEventTypeEnums.TASKSTART.toString)

    Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      stage.activeTasks += 1
      stage.firstLaunchTime = math.min(stage.firstLaunchTime, event.taskInfo.launchTime)

      val locality = event.taskInfo.taskLocality.toString()
      val count = stage.localitySummary.getOrElse(locality, 0L) + 1L
      stage.localitySummary = stage.localitySummary ++ Map(locality -> count)
      stage.activeTasksPerExecutor(event.taskInfo.executorId) += 1
      liveUpdate(stage, MetricEventNameEnums.STAGE.toString, MetricEventTypeEnums.METRICSUPDATE.toString)

      stage.jobs.foreach { job =>
        job.activeTasks += 1
        liveUpdate(job, MetricEventNameEnums.JOB.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
      }
    }

    liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      exec.activeTasks += 1
      exec.totalTasks += 1
      exec.appId = appInfo.appId
      exec.id = appInfo.appId + "-" + exec.executorId
      liveUpdate(exec, MetricEventNameEnums.EXECUTOR.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
    }
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = {
    // Call update on the task so that the "getting result" time is written to the store; the
    // value is part of the mutable TaskInfo state that the live entity already references.
    liveTasks.get(event.taskInfo.taskId).foreach { task =>
      liveUpdate(task, MetricEventNameEnums.TASK.toString, MetricEventTypeEnums.TASKGETTINGRESULT.toString)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    // TODO: can this really happen?
    if (event.taskInfo == null) {
      return
    }
    val metricsDelta = liveTasks.remove(event.taskInfo.taskId).map { task =>

      task.errorMessage = event.reason match {
        case Success =>
          None
        case k: TaskKilled =>
          Some(k.reason)
        case e: ExceptionFailure => // Handle ExceptionFailure because we might have accumUpdates
          Some(e.toErrorString)
        case e: TaskFailedReason => // All other failure cases
          Some(e.toErrorString)
        case other =>
          logInfo(s"Unhandled task end reason: $other")
          None
      }
      val delta = task.updateMetrics(event.taskMetrics)
      liveUpdate(task, MetricEventNameEnums.TASK.toString, MetricEventTypeEnums.TASKEND.toString)
      delta
    }.orNull

    val (completedDelta, failedDelta, killedDelta) = event.reason match {
      case Success =>
        (1, 0, 0)
      case _: TaskKilled =>
        (0, 0, 1)
      case _: TaskCommitDenied =>
        (0, 0, 1)
      case _ =>
        (0, 1, 0)
    }

    Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      if (metricsDelta != null) {
        stage.metrics = LiveEntityHelpers.addMetrics(stage.metrics, metricsDelta)
      }
      stage.activeTasks -= 1
      stage.completedTasks += completedDelta
      if (completedDelta > 0) {
        stage.completedIndices.add(event.taskInfo.index)
      }
      stage.failedTasks += failedDelta
      stage.killedTasks += killedDelta
      if (killedDelta > 0) {
        stage.killedSummary = killedTasksSummary(event.reason, stage.killedSummary)
      }
      stage.activeTasksPerExecutor(event.taskInfo.executorId) -= 1
      // [SPARK-24415] Wait for all tasks to finish before removing stage from live list
      val removeStage =
        stage.activeTasks == 0 &&
          (v1.StageStatus.COMPLETE.equals(stage.status) ||
            v1.StageStatus.FAILED.equals(stage.status))

      liveUpdate(stage, MetricEventNameEnums.STAGE.toString, MetricEventTypeEnums.METRICSUPDATE.toString)

      // Store both stage ID and task index in a single long variable for tracking at job level.
      val taskIndex = (event.stageId.toLong << Integer.SIZE) | event.taskInfo.index
      stage.jobs.foreach { job =>
        job.activeTasks -= 1
        job.completedTasks += completedDelta
        if (completedDelta > 0) {
          job.completedIndices.add(taskIndex)
        }
        job.failedTasks += failedDelta
        job.killedTasks += killedDelta
        if (killedDelta > 0) {
          job.killedSummary = killedTasksSummary(event.reason, job.killedSummary)
        }
        liveUpdate(job, MetricEventNameEnums.JOB.toString, MetricEventTypeEnums.JOBEND.toString)
      }

      val esummary = stage.executorSummary(event.taskInfo.executorId)
      esummary.taskTime += event.taskInfo.duration
      esummary.succeededTasks += completedDelta
      esummary.failedTasks += failedDelta
      esummary.killedTasks += killedDelta
      if (metricsDelta != null) {
        esummary.metrics = LiveEntityHelpers.addMetrics(esummary.metrics, metricsDelta)
      }

      val isLastTask = stage.activeTasksPerExecutor(event.taskInfo.executorId) == 0
      // If the last task of the executor finished, then update the esummary
      // for both live and history events.
      if (isLastTask) {
        liveUpdate(esummary, MetricEventNameEnums.EXECUTORSTAGESUMMARY.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
      }

      if (removeStage) {
        liveStages.remove((event.stageId, event.stageAttemptId))
      }
    }


    liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      exec.activeTasks -= 1
      exec.completedTasks += completedDelta
      exec.failedTasks += failedDelta
      exec.totalDuration += event.taskInfo.duration

      // Note: For resubmitted tasks, we continue to use the metrics that belong to the
      // first attempt of this task. This may not be 100% accurate because the first attempt
      // could have failed half-way through. The correct fix would be to keep track of the
      // metrics added by each attempt, but this is much more complicated.
      if (event.reason != Resubmitted) {
        if (event.taskMetrics != null) {
          val readMetrics = event.taskMetrics.shuffleReadMetrics
          exec.totalGcTime += event.taskMetrics.jvmGCTime
          exec.totalInputBytes += event.taskMetrics.inputMetrics.bytesRead
          exec.totalShuffleRead += readMetrics.localBytesRead + readMetrics.remoteBytesRead
          exec.totalShuffleWrite += event.taskMetrics.shuffleWriteMetrics.bytesWritten
        }
      }
      liveUpdate(exec, MetricEventNameEnums.EXECUTOR.toString, MetricEventTypeEnums.METRICSUPDATE.toString)

    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val maybeStage =
      Option(liveStages.get((event.stageInfo.stageId, event.stageInfo.attemptNumber)))
    maybeStage.foreach { stage =>
      stage.info = event.stageInfo

      // We have to update the stage status AFTER we create all the executorSummaries
      // because stage deletion deletes whatever summaries it finds when the status is completed.
      stage.executorSummaries.values.foreach(liveUpdate(_, MetricEventNameEnums.EXECUTORSTAGESUMMARY.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
      )

      // Because of SPARK-20205, old event logs may contain valid stages without a submission time
      // in their start event. In those cases, we can only detect whether a stage was skipped by
      // waiting until the completion event, at which point the field would have been set.
      stage.status = event.stageInfo.failureReason match {
        case Some(_) => v1.StageStatus.FAILED
        case _ if event.stageInfo.submissionTime.isDefined => v1.StageStatus.COMPLETE
        case _ => v1.StageStatus.SKIPPED
      }

      stage.jobs.foreach { job =>
        stage.status match {
          case v1.StageStatus.COMPLETE =>
            job.completedStages += event.stageInfo.stageId
          case v1.StageStatus.SKIPPED =>
            job.skippedStages += event.stageInfo.stageId
            job.skippedTasks += event.stageInfo.numTasks
          case _ =>
            job.failedStages += 1
        }
        job.activeStages -= 1
        liveUpdate(job, MetricEventNameEnums.JOB.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
      }

//      pools.get(stage.schedulingPool).foreach { pool =>
//        pool.stageIds = pool.stageIds - event.stageInfo.stageId
//        update(pool, now)
//      }

//      val executorIdsForStage = stage.blackListedExecutors
//      executorIdsForStage.foreach { executorId =>
//        liveExecutors.get(executorId).foreach { exec =>
//          removeBlackListedStageFrom(exec, event.stageInfo.stageId, now)
//        }
//      }

      // Remove stage only if there are no active tasks remaining
      val removeStage = stage.activeTasks == 0
      update(stage, MetricEventNameEnums.STAGE.toString, MetricEventTypeEnums.STAGEEND.toString)
      if (removeStage) {
        liveStages.remove((event.stageInfo.stageId, event.stageInfo.attemptNumber))
      }
    }
  }


  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    // This needs to set fields that are already set by onExecutorAdded because the driver is
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    // Nothing to do here. Covered by onExecutorRemoved.
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    event.accumUpdates.foreach { case (taskId, sid, sAttempt, accumUpdates) =>
      liveTasks.get(taskId).foreach { task =>
        val metrics = TaskMetrics.fromAccumulatorInfos(accumUpdates)
        val delta = task.updateMetrics(metrics)
        liveUpdate(task, MetricEventNameEnums.TASK.toString, MetricEventTypeEnums.METRICSUPDATE.toString)

        Option(liveStages.get((sid, sAttempt))).foreach { stage =>
          stage.metrics = LiveEntityHelpers.addMetrics(stage.metrics, delta)
          liveUpdate(stage, MetricEventNameEnums.STAGE.toString, MetricEventTypeEnums.METRICSUPDATE.toString)

          val esummary = stage.executorSummary(event.execId)
          esummary.metrics = LiveEntityHelpers.addMetrics(esummary.metrics, delta)
          liveUpdate(esummary, MetricEventNameEnums.EXECUTORSTAGESUMMARY.toString, MetricEventTypeEnums.METRICSUPDATE.toString)
        }
      }
    }

    // Flush updates if necessary. Executor heartbeat is an event that happens periodically. Flush
    // here to ensure the staleness of Spark UI doesn't last more than
    // `max(heartbeat interval, liveUpdateMinFlushPeriod)`.
//    if (now - lastFlushTimeNs > liveUpdateMinFlushPeriod) {
//      flush(maybeUpdate(_, now))
//      // Re-get the current system time because `flush` may be slow and `now` is stale.
//      lastFlushTimeNs = System.nanoTime()
//    }
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {

  }

  /** Go through all `LiveEntity`s and use `entityFlushFunc(entity)` to flush them. */
//  private def flush(entityFlushFunc: LiveMetricEntity => Unit): Unit = {
//    liveStages.values.asScala.foreach { stage =>
//      entityFlushFunc(stage)
//      stage.executorSummaries.values.foreach(entityFlushFunc)
//    }
//    liveJobs.values.foreach(entityFlushFunc)
//    liveExecutors.values.foreach(entityFlushFunc)
//    liveTasks.values.foreach(entityFlushFunc)
//  }

  /**
   * Shortcut to get active stages quickly in a live application, for use by the console
   * progress bar.
   */
//  def activeStages(): Seq[v1.StageData] = {
//    liveStages.values.asScala
//      .filter(_.info.submissionTime.isDefined)
//      .map(_.toApi())
//      .toList
//      .sortBy(_.stageId)
//  }

  /**
   * Apply a delta to a value, but ensure that it doesn't go negative.
   */
  private def addDeltaToValue(old: Long, delta: Long): Long = math.max(0, old + delta)



  private def getOrCreateExecutor(executorId: String, addTime: Long): LiveMetricExecutor = {
    liveExecutors.getOrElseUpdate(executorId, {
      activeExecutorCount += 1
      new LiveMetricExecutor(executorId, addTime)
    })
  }


  private def updateExecutorMemoryDiskInfo(
      exec: LiveExecutor,
      storageLevel: StorageLevel,
      memoryDelta: Long,
      diskDelta: Long): Unit = {
    if (exec.hasMemoryInfo) {
      if (storageLevel.useOffHeap) {
        exec.usedOffHeap = addDeltaToValue(exec.usedOffHeap, memoryDelta)
      } else {
        exec.usedOnHeap = addDeltaToValue(exec.usedOnHeap, memoryDelta)
      }
    }
    exec.memoryUsed = addDeltaToValue(exec.memoryUsed, memoryDelta)
    exec.diskUsed = addDeltaToValue(exec.diskUsed, diskDelta)
  }

  private def getOrCreateStage(info: StageInfo): LiveMetricStage = {
    val stage = liveStages.computeIfAbsent((info.stageId, info.attemptNumber),
      new Function[(Int, Int), LiveMetricStage]() {
        override def apply(key: (Int, Int)): LiveMetricStage = new LiveMetricStage(appInfo.appId)
      })
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

  private def update(entity: LiveMetricEntity, eventName: String, eventType: String): Unit = {
    entity.report(kafkaSink, eventName, eventType)
  }

  /** Update an entity only if in a live app; avoids redundant writes when replaying logs. */
  private def liveUpdate(entity: LiveMetricEntity, eventName: String, eventType: String): Unit = {
    if (live) {
      update(entity, eventName, eventType)
    }
  }

//  /** Update a live entity only if it hasn't been updated in the last configured period. */
//  private def maybeUpdate(entity: LiveMetricEntity, now: Long, eventName: String, eventType: String): Unit = {
//    if (live && liveUpdatePeriodNs >= 0 && now - entity.lastWriteTime > liveUpdatePeriodNs) {
//      update(entity, now, )
//    }
//  }


  object MetricEventNameEnums extends Enumeration {
    type MetricEventEnums = Value
    val APPLICATION, JOB, STAGE, EXECUTOR, TASK, EXECUTORSTAGESUMMARY = Value
  }

  object MetricEventTypeEnums extends Enumeration {
    type MetricEventEnums = Value
    val APPLICATIONSTART, APPLICATIONEND,JOBSTART, JOBEND, STAGESTART, STAGEEND, EXECUTORSTART,
    EXECUTOREND, TASKSTART, TASKEND, METRICSUPDATE, TASKGETTINGRESULT = Value
  }

  def close(): Unit = {
    kafkaSink.close()
    liveExecutors.clear()
    liveJobs.clear()
    liveStages.clear()
    liveTasks.clear()
  }

}

private[spark] object MetricsListener {

  /**
    * Create an in-memory store for a live application.
    */
  def createLiveRecord(conf: SparkConf): MetricsListener = {
    val kafkaSink = new KafkaSink(conf)
    new MetricsListener(conf, kafkaSink, true)
  }
}
