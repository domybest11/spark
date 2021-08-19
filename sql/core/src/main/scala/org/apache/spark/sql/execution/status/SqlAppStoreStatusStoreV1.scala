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

import java.util.{List => JList}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.ipc.RPC
import org.apache.hadoop.yarn.api.ApplicationClientProtocol
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.client.ClientRMProxy
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.JobExecutionStatus
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{AppClientStatus, SQLAppStatusStore, SQLExecutionUIData}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{JobData, StageData, _}
import org.apache.spark.ui.scope.RDDOperationGraph
import org.apache.spark.util.{AppCostReporter, SqlTextTruncate, Utils}

import scala.util.control.Breaks.{break, breakable}

/**
 * A wrapper around a KVStore that provides methods for accessing the API data stored within.
 */
class SqlAppStoreStatusStoreV1(
    val sqlAppStatusStore: Option[SQLAppStatusStore] = None,
    val appStatusStore: Option[AppStatusStore] = None,
    val conf: SparkConf) extends Logging {

  private val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(conf))
  private val reporter = AppCostReporter.createAppCostReporter(conf)

  def countResourceCost(applicationSQLExecutionData: ApplicationSQLExecutionData): String = {
    var rmClient: ApplicationClientProtocol = null
    try {
    rmClient = ClientRMProxy.createRMProxy(hadoopConf, classOf[ApplicationClientProtocol])
    val request = Records.newRecord(classOf[GetApplicationReportRequest])
    val applicationId = ApplicationId.fromString(conf.get("spark.app.id"))
    request.setApplicationId(applicationId)
    val resourceUsage = rmClient.getApplicationReport(request).getApplicationReport
      .getApplicationResourceUsageReport
    val memorySeconds = resourceUsage.getMemorySeconds
    val vcoreSeconds = resourceUsage.getVcoreSeconds
    var inputBytes: Long = 0
    var outputBytes: Long = 0
    val jobs: ListBuffer[SQLJobData] = applicationSQLExecutionData.sqlExecutionData.get.jobs
    var costMessage = ""
      val action = "resource"
      val statement = applicationSQLExecutionData.sqlExecutionData.get.statement
    if (jobs.nonEmpty) {
        jobs.foreach(job => {
          inputBytes += job.stages.filter(_.inComing == 0).map(_.inputBytes).toList.sum
          outputBytes += job.stages.filter(_.outGoing == 0).map(_.outputBytes).toList.sum
        })
        val inputUnit = if (inputBytes > 0) Utils.bytesToString(inputBytes) else "0B"
        val outputUnit = if (outputBytes > 0) Utils.bytesToString(outputBytes) else "0B"
        var traceId = conf.get("spark.trace.id", "")
        if (traceId.isEmpty) {
          traceId = ""
        }
        // scalastyle:off
         costMessage = s"使用资源消耗情况(${SqlTextTruncate.getSqlIdentifier(statement).get}), 内存: ${memorySeconds}(m*s)," +
          s" CPU: ${vcoreSeconds}(c*s), 读数据量: ${inputUnit}, 写数据量: ${outputUnit}"

        val tags = Map[String, Any]("memory" -> memorySeconds, "cpu" -> vcoreSeconds, "readDataSize" -> inputBytes, "writeDataSize" -> outputBytes)
        reporter.postEvent(Some(traceId), action, "", costMessage, tags, System.currentTimeMillis())

        logInfo(costMessage)
      }
      costMessage
    } catch {
       case e: Exception =>
       logWarning("Get application resource usage failed", e)
        ""
    } finally {
    if (rmClient != null) {
      RPC.stopProxy(rmClient)
    }
      rmClient = null
    }
  }

  def assembleExecutionInfo(
      executionInfo: SQLExecutionUIData, method: String): Option[ApplicationSQLExecutionData] = {
    val applicationInfoV1 = applicationInfo().get
    val sparkPropertyMap = environmentInfo().get.sparkProperties.toMap
    val applicationSQLExecutionData = new ApplicationSQLExecutionData(
      applicationInfoV1.id,
      applicationInfoV1.name,
      applicationInfoV1.attempts.last.attemptId.getOrElse("0"),
      sparkPropertyMap("spark.driver.host"),
      sparkPropertyMap("spark.yarn.queue"),
      "",
      Some(applicationInfoV1.attempts.last.startTime).get.getTime,
      0,
      0,
      sparkPropertyMap("spark.driver.host"))
    var executionsLists: List[SQLExecutionUIData] = List.empty
    if (executionInfo.executionId == AppClientStatus.APP_CLIENT_END) {
      executionsLists = executionsList()
    } else {
      executionsLists = executionsLists :+ executionInfo
    }
    val listExecutionData = new ListBuffer[ExecutionData]
    executionsLists.foreach(executionInfo => {
      val listSQLJobData = new ListBuffer[SQLJobData]
      executionInfo.jobs.keys.foreach(jobid => {
        val jobData = job(jobid)
        if (jobData.isDefined) {
          val listSQLStageData = new ListBuffer[SQLStageData]
          jobData.get.stageIds.foreach(stageId => {
            val stageInfo = stageData(stageId, true)
            stageInfo.seq.foreach(stageData => {
              listSQLStageData.append(buildSQLStageData(stageData).get)
            })
          })
          listSQLJobData.append(buildSQLJobData(listSQLStageData, jobData.get).get)
        }
      })
      val user = if (sys.props("user.name").nonEmpty) sys.props("user.name") else ""
      val statement = if (listSQLJobData.nonEmpty && listSQLJobData.head.stages.nonEmpty) {
        listSQLJobData.head.stages.head.description.getOrElse("")
      } else {
        ""
      }
      val executionData = new ExecutionData(statement, executionInfo.submissionTime, user)
      executionData.finishTimestamp = executionInfo.completionTime.map(_.getTime).getOrElse(0L)
      executionData.executePlan = executionInfo.physicalPlanDescription
      executionData.detail = executionInfo.details
      executionData.executionId = executionInfo.executionId
      val isRunning = executionInfo.completionTime.isEmpty ||
        executionInfo.jobs.exists { case (_, status) => status == JobExecutionStatus.RUNNING }
      val isFailed =
        executionInfo.jobs.exists { case (_, status) => status == JobExecutionStatus.FAILED }
      val state = if (isRunning) {
        "RUNNING"
      } else if (isFailed) {
        "FAILED"
      } else {
        "FINISHED"
      }
      executionData.state = state
      executionData.jobId = ListBuffer.empty ++ executionInfo.jobs.keys.map(_.toString).toList
      executionData.groupId = if (listSQLJobData.nonEmpty) listSQLJobData(0).jobGroup else ""
      executionData.executionId = executionInfo.executionId
      executionData.finishAndReport = executionInfo.finishAndReport
      executionData.duration = executionData.totalTime
      executionData.jobs = listSQLJobData
      executionData.statement = executionInfo.statement
      applicationSQLExecutionData.user = user
      listExecutionData.append(executionData)
    })
    if (executionInfo.executionId == AppClientStatus.APP_CLIENT_END) {
      applicationSQLExecutionData.sqlExecutionDatas = Some(listExecutionData)
    } else {
      applicationSQLExecutionData.sqlExecutionData = Some(listExecutionData(0))
    }
    applicationSQLExecutionData.currentTime = System.currentTimeMillis()
    applicationSQLExecutionData.endTime =
      Some(applicationInfo().get.attempts.last.endTime).get.getTime
    applicationSQLExecutionData.status = if (applicationInfo().get.attempts.last.completed) {
      MetricEventTypeEnums.APPLICATIONEND.toString
    } else {
      MetricEventTypeEnums.APPLICATIONSRUNNING.toString
    }
    applicationSQLExecutionData.tarceId = if (sparkPropertyMap.contains("spark.trace.id")) {
      sparkPropertyMap("spark.trace.id")
    } else {
      ""
    }
    if (executionInfo.executionId !=  AppClientStatus.APP_CLIENT_END
      && executionInfo.completionTime.nonEmpty && method.equals("once")) {
      applicationSQLExecutionData.executionCost = countResourceCost(applicationSQLExecutionData)
      if (executionInfo.executionId == AppClientStatus.SHUFFLE_FAIL) {
        applicationSQLExecutionData.shuffleFailed = executionInfo.description
      }
    }
    Some(applicationSQLExecutionData)
  }

  def buildSQLJobData(listSQLStageData: ListBuffer[SQLStageData],
      jobData: JobData): Option[SQLJobData] = {
    val sqlJobData = new SQLJobData(
      jobData.jobId,
      jobData.jobGroup.getOrElse(""),
      jobData.description.getOrElse(""),
      jobData.submissionTime.get.getTime,
      if (jobData.completionTime.isDefined) {
        jobData.completionTime.get.getTime
      } else {
        0L
      },
      jobData.status.toString,
      jobData.stageIds,
      jobData.numTasks,
      jobData.numActiveTasks,
      jobData.numCompletedTasks,
      jobData.numSkippedTasks,
      jobData.numFailedTasks,
      jobData.numKilledTasks,
      jobData.numCompletedIndices,
      jobData.numActiveStages,
      jobData.numCompletedStages,
      jobData.numSkippedStages,
      jobData.numFailedStages,
      listSQLStageData)
    Some(sqlJobData)
  }

  def buildSQLStageData(stageData: StageData): Option[SQLStageData] = {
    val metrics: Option[TaskMetricDistributions] = taskSummary(
      stageData.stageId, stageData.attemptId, Array(0, 0.25, 0.5, 0.75, 1.0))
    var sqlStageData: SQLStageData = null
    if (!metrics.isDefined) {
      sqlStageData = new SQLStageData(stageData.status, stageData.stageId, stageData.attemptId,
        stageData.numTasks, stageData.numActiveTasks)
      Some(sqlStageData)
    } else {
      val tasksLocalitySeq = tasksLocalitySummary(stageData.stageId,
        stageData.attemptId, stageData.numTasks)
      val taskMetricSummary: TaskMetricSummary = taskMaxAvgSummary(stageData)
      val sqlTaskAvgMetrics = new SQLTaskAvgMetrics(
        metrics.get.executorDeserializeTime.apply(2),
        metrics.get.executorDeserializeCpuTime.apply(2),
        taskMetricSummary.avgExecutorCpuTime,
        taskMetricSummary.avgExecutorRunTime,
        metrics.get.resultSize.apply(2),
        taskMetricSummary.avgJvmGcTime,
        metrics.get.resultSerializationTime.apply(2),
        metrics.get.memoryBytesSpilled.apply(2),
        taskMetricSummary.avgDiskBytesSpilled,
        metrics.get.peakExecutionMemory.apply(2),
        metrics.get.inputMetrics.bytesRead.apply(2),
        metrics.get.inputMetrics.recordsRead.apply(2),
        metrics.get.outputMetrics.bytesWritten.apply(2),
        metrics.get.outputMetrics.recordsWritten.apply(2),
        metrics.get.shuffleWriteMetrics.writeBytes.apply(2),
        metrics.get.shuffleWriteMetrics.writeTime.apply(2),
        metrics.get.shuffleWriteMetrics.writeRecords.apply(2),
        metrics.get.shuffleReadMetrics.remoteBlocksFetched.apply(2),
        metrics.get.shuffleReadMetrics.localBlocksFetched.apply(2),
        metrics.get.shuffleReadMetrics.fetchWaitTime.apply(2),
        taskMetricSummary.avgShuffleRemoteBytesRead,
        metrics.get.shuffleReadMetrics.remoteBytesReadToDisk.apply(2),
        metrics.get.shuffleReadMetrics.readBytes.apply(2),
        metrics.get.shuffleReadMetrics.readRecords.apply(2),
        taskMetricSummary.avgDuration,
        metrics.get.schedulerDelay.apply(2),
        taskMetricSummary.avgShuffleReadBytes
      )

      val sqlTaskMaxMetrics = new SQLTaskMaxMetrics(
        metrics.get.executorDeserializeTime.apply(4),
        metrics.get.executorDeserializeCpuTime.apply(4),
        metrics.get.executorCpuTime.apply(4),
        metrics.get.executorRunTime.apply(4),
        metrics.get.resultSize.apply(4),
        metrics.get.jvmGcTime.apply(4),
        metrics.get.resultSerializationTime.apply(4),
        metrics.get.memoryBytesSpilled.apply(4),
        metrics.get.diskBytesSpilled.apply(4),
        metrics.get.peakExecutionMemory.apply(4),
        metrics.get.inputMetrics.bytesRead.apply(4),
        metrics.get.inputMetrics.recordsRead.apply(4),
        metrics.get.outputMetrics.bytesWritten.apply(4),
        metrics.get.outputMetrics.recordsWritten.apply(4),
        metrics.get.shuffleWriteMetrics.writeBytes.apply(4),
        metrics.get.shuffleWriteMetrics.writeTime.apply(4),
        metrics.get.shuffleWriteMetrics.writeRecords.apply(4),
        metrics.get.shuffleReadMetrics.remoteBlocksFetched.apply(4),
        metrics.get.shuffleReadMetrics.localBlocksFetched.apply(4),
        metrics.get.shuffleReadMetrics.fetchWaitTime.apply(4),
        metrics.get.shuffleReadMetrics.remoteBytesRead.apply(4),
        metrics.get.shuffleReadMetrics.remoteBytesReadToDisk.apply(4),
        metrics.get.shuffleReadMetrics.readBytes.apply(4),
        metrics.get.shuffleReadMetrics.readRecords.apply(4),
        taskMetricSummary.maxDuration,
        metrics.get.schedulerDelay.apply(4),
        taskMetricSummary.maxShuffleReadBytes)
      val skewKeys = stageData.accumulatorUpdates
        .filter(acc => acc.name == TaskMetrics.DATA_SKEW_KEYS).map(acc => acc.value).seq
      sqlStageData = new SQLStageData(
        stageData.status,
        stageData.stageId,
        stageData.attemptId,
        stageData.numTasks,
        stageData.numActiveTasks,
        stageData.numCompleteTasks,
        stageData.numFailedTasks,
        stageData.numKilledTasks,
        stageData.numCompletedIndices,
        stageData.executorRunTime,
        stageData.executorCpuTime,
        stageData.submissionTime.get.getTime,
        stageData.firstTaskLaunchedTime.get.getTime,
        if (stageData.completionTime.isDefined) {
          stageData.completionTime.get.getTime
        } else {
          0L
        },
        stageData.failureReason,
        stageData.inputBytes,
        stageData.inputRecords,
        stageData.outputBytes,
        stageData.outputRecords,
        stageData.shuffleReadBytes,
        stageData.shuffleReadRecords,
        stageData.shuffleWriteBytes,
        stageData.shuffleWriteRecords,
        stageData.name,
        stageData.description,
        stageData.details,
        tasksLocalitySeq.apply(0),
        tasksLocalitySeq.apply(1),
        tasksLocalitySeq.apply(2),
        tasksLocalitySeq.apply(3),
        Some(sqlTaskAvgMetrics),
        Some(sqlTaskMaxMetrics),
        skewKeys,
        operationGraphForStage(stageData.stageId).incomingEdges.size,
        operationGraphForStage(stageData.stageId).outgoingEdges.size)
      Some(sqlStageData)
    }
  }

  def applicationInfo(): Option[ApplicationInfo] = {
    try {
      Some(appStatusStore.get.applicationInfo())
    } catch {
      case _: Exception =>
        logWarning("can not find the sql applicationInfo status")
        None
    }
  }

  def environmentInfo(): Option[ApplicationEnvironmentInfo] = {
    Some(appStatusStore.get.environmentInfo())
  }

  def executionsList(): List[SQLExecutionUIData] = {
    try {
      sqlAppStatusStore.get.executionsList().toList
        .filterNot(sqlExecution => sqlExecution.statement == null ||
        sqlExecution.statement.equals(""))
    } catch {
      case _: NoSuchElementException =>
        logWarning("can not find the sql executions status")
        List.empty
    }
  }

  def execution(executionId: Long): Option[SQLExecutionUIData] = {
    try {
      sqlAppStatusStore.get.execution(executionId)
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def executionsCount(): Long = {
    sqlAppStatusStore.get.executionsCount()
  }

  def jobsList(statuses: JList[JobExecutionStatus]): Seq[JobData] = {
    appStatusStore.get.jobsList(statuses)
  }

  def job(jobId: Int): Option[JobData] = {
    try {
      Some(appStatusStore.get.job(jobId))
    } catch {
      case _: NoSuchElementException =>
        logWarning("can not find the sql job status")
        None
    }
  }

  def executorList(activeOnly: Boolean): Seq[ExecutorSummary] = {
    appStatusStore.get.executorList(activeOnly)
  }

  def executorSummary(executorId: String): ExecutorSummary = {
    appStatusStore.get.executorSummary(executorId)
  }

  /**
   * This is used by ConsoleProgressBar to quickly fetch active stages for drawing the progress
   * bar. It will only return anything useful when called from a live application.
   */
  def activeStages(): Seq[StageData] = {
    appStatusStore.get.activeStages()
  }

  def stageList(statuses: JList[StageStatus]): Seq[StageData] = {
    appStatusStore.get.stageList(statuses)
  }

  def stageData(stageId: Int, details: Boolean = false): Seq[StageData] = {
    try {
      appStatusStore.get.stageData(stageId, details)
    } catch {
      case _: NoSuchElementException =>
        logWarning("can not find the sql stageData status")
        Seq.empty
    }
  }

  def operationGraphForStage(stageId: Int): RDDOperationGraph = {
    appStatusStore.get.operationGraphForStage(stageId)
  }

  def lastStageAttempt(stageId: Int): StageData = {
    appStatusStore.get.lastStageAttempt(stageId)
  }

  def stageAttempt(stageId: Int, stageAttemptId: Int,
                   details: Boolean = false): (StageData, Seq[Int]) = {
    appStatusStore.get.stageAttempt(stageId, stageAttemptId, details)
  }

  def taskCount(stageId: Int, stageAttemptId: Int): Long = {
    appStatusStore.get.taskCount(stageId, stageAttemptId)
  }

  def localitySummary(stageId: Int, stageAttemptId: Int): Map[String, Long] = {
    appStatusStore.get.localitySummary(stageId, stageAttemptId)
  }

  /**
   * Calculates a summary of the task metrics for the given stage attempt, returning the
   * requested quantiles for the recorded metrics.
   *
   * This method can be expensive if the requested quantiles are not cached; the method
   * will only cache certain quantiles (every 0.05 step), so it's recommended to stick to
   * those to avoid expensive scans of all task data.
   */
  def taskSummary(
      stageId: Int,
      stageAttemptId: Int,
      unsortedQuantiles: Array[Double]): Option[TaskMetricDistributions] = {
    appStatusStore.get.taskSummary(stageId, stageAttemptId, unsortedQuantiles)
  }

  case class TaskMetricSummary(var maxDuration: Double = 0,
      var avgDuration: Double = 0,
      var maxShuffleReadBytes: Double = 0,
      var avgShuffleReadBytes: Double = 0,
      var avgDiskBytesSpilled: Double = 0,
      var avgShuffleRemoteBytesRead: Double = 0,
      var avgExecutorRunTime: Double = 0,
      var avgExecutorCpuTime: Double = 0,
      var avgJvmGcTime: Double = 0
  )


  def taskMaxAvgSummary(stageData: StageData): TaskMetricSummary = {
    val numCompleteTasks = stageData.numCompleteTasks
    val toltalTasks = appStatusStore.get.taskList(stageData.stageId,
      stageData.attemptId, Int.MaxValue)

    val durationsRunning = toltalTasks.filter(_.status == "RUNNING").map(task =>
      System.currentTimeMillis() - task.launchTime.getTime)
    val durationsSuccess = toltalTasks.filter(_.status == "SUCCESS").map(task => task.duration.get)
    val durations = durationsRunning ++ durationsSuccess

    val shuffleReadBytesRunning = toltalTasks.filter(task => (task.status == "RUNNING") &&
      task.taskMetrics.isDefined).
      map(task => task.taskMetrics.get.shuffleReadMetrics.localBytesRead +
        task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead)
    val shuffleReadBytesSuccess = toltalTasks.filter(task => (task.status == "SUCCESS") &&
      task.taskMetrics.isDefined).
      map(task => task.taskMetrics.get.shuffleReadMetrics.localBytesRead +
        task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead)
    val shuffleReadBytes = shuffleReadBytesRunning ++ shuffleReadBytesSuccess

    val diskBytesSpilled = toltalTasks.filter(task => (task.status == "SUCCESS") &&
      task.taskMetrics.isDefined).map(task => task.taskMetrics.get.diskBytesSpilled)

    val shuffleRemoteBytesRead = toltalTasks.filter(task => (task.status == "SUCCESS") &&
      task.taskMetrics.isDefined).map(task =>
      task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead)

    val executorRunTime = toltalTasks.filter(task => (task.status == "SUCCESS") &&
      task.taskMetrics.isDefined).map(task =>
      task.taskMetrics.get.executorRunTime)

    val executorCpuTime = toltalTasks.filter(task => (task.status == "SUCCESS") &&
      task.taskMetrics.isDefined).map(task =>
      task.taskMetrics.get.executorCpuTime)

    val jvmGcTime = toltalTasks.filter(task => (task.status == "SUCCESS") &&
      task.taskMetrics.isDefined).map(task =>
      task.taskMetrics.get.jvmGcTime)

    TaskMetricSummary(durations.maxBy(_.toDouble),
      safeDivide(durationsSuccess.sum.toDouble, numCompleteTasks).formatted("%.2f").toDouble,
      shuffleReadBytes.maxBy(_.toDouble),
      safeDivide(shuffleReadBytesSuccess.sum.toDouble, numCompleteTasks).formatted("%.2f").toDouble,
      safeDivide(diskBytesSpilled.sum.toDouble, numCompleteTasks).formatted("%.2f").toDouble,
      safeDivide(shuffleRemoteBytesRead.sum.toDouble, numCompleteTasks).formatted("%.2f").toDouble,
      safeDivide(executorRunTime.sum.toDouble, numCompleteTasks).formatted("%.2f").toDouble,
      safeDivide(executorCpuTime.sum.toDouble, numCompleteTasks).formatted("%.2f").toDouble,
      safeDivide(jvmGcTime.sum.toDouble, numCompleteTasks).formatted("%.2f").toDouble
    )
  }

  def safeDivide(divisor: Double, dividend: Long): Double = {
    if (dividend == 0L) 0L else divisor / dividend
  }

  def tasksLocalitySummary(stageId: Int, stageAttemptId: Int, maxTasks: Int): Seq[Double] = {
    val taskDataList = taskList(stageId, stageAttemptId, maxTasks)
    var taskProcessLocal: Double = 0.0
    var taskNoPref: Double = 0.0
    var taskRackLocal: Double = 0.0
    var taskAnyRate: Double = 0.0

    taskDataList.foreach(taskData => {
      val locality = taskData.taskLocality
      locality match {
        case TaskLocality.ANY => taskAnyRate += 1
        case TaskLocality.PROCESS_LOCAL => taskProcessLocal += 1
        case TaskLocality.NO_PREF => taskNoPref += 1
        case TaskLocality.RACK_LOCAL => taskRackLocal += 1
        case TaskLocality.NODE_LOCAL => logDebug("locality match: " + locality)
        case _ => logDebug("locality no match: " + locality)
      }
    })
    val taskProcessLocalRate = taskProcessLocal./(taskDataList.size)
    val taskNoPrefRate = taskNoPref./(taskDataList.size)
    val taskRackLocalRate = taskRackLocal./(taskDataList.size)
    val taskAnyRateRate = taskAnyRate./(taskDataList.size)
    Seq(taskProcessLocalRate, taskNoPrefRate, taskRackLocalRate, taskAnyRateRate)
  }

  def taskList(stageId: Int, stageAttemptId: Int, maxTasks: Int): Seq[TaskData] = {
    appStatusStore.get.taskList(stageId, stageAttemptId, maxTasks)
  }

  def taskList(
      stageId: Int,
      stageAttemptId: Int,
      offset: Int,
      length: Int,
      sortBy: Option[String],
      ascending: Boolean): Seq[TaskData] = {
    appStatusStore.get.taskList(stageId, stageAttemptId, offset, length, sortBy, ascending)
  }

  def executorSummary(stageId: Int, attemptId: Int): Map[String, ExecutorStageSummary] = {
    appStatusStore.get.executorSummary(stageId, attemptId)
  }

  object MetricEventTypeEnums extends Enumeration {
    type MetricEventEnums = Value
    val APPLICATIONSTART, APPLICATIONSRUNNING, APPLICATIONEND,
    JOBSTART, JOBEND, STAGESTART, STAGEEND, EXECUTORSTART,
    EXECUTOREND, TASKSTART, TASKEND, METRICSUPDATE, TASKGETTINGRESULT = Value
  }

  object TaskLocality {
    val PROCESS_LOCAL = "PROCESS_LOCAL"
    val NODE_LOCAL = "NODE_LOCAL"
    val NO_PREF = "NO_PREF"
    val RACK_LOCAL = "RACK_LOCAL"
    val ANY = "ANY"
  }
}

