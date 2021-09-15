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

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockManagerAdded, SparkListenerBlockManagerRemoved, SparkListenerBlockUpdated, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerExecutorAdded, SparkListenerExecutorExcluded, SparkListenerExecutorExcludedForStage, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerExecutorReportInfo, SparkListenerExecutorUnexcluded, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerNodeExcluded, SparkListenerNodeExcludedForStage, SparkListenerNodeUnexcluded, SparkListenerResourceProfileAdded, SparkListenerSpeculativeTaskSubmitted, SparkListenerStageCompleted, SparkListenerStageExecutorMetrics, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, SparkListenerUnpersistRDD, SparkListenerUnschedulableTaskSetAdded, SparkListenerUnschedulableTaskSetRemoved}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.{ApplicationDataRecord, ExceptionRecord, ExecutionDataRecord, KafkaProducerUtil}
import org.apache.spark.util.ExceptionType.SHUFFLE_FAIL


class SqlMarioListener(conf: SparkConf,
                       kvstore: ElementTrackingStore,
                       live: Boolean) extends SparkListener with Logging {
  val TRACE_ID_KEY = "spark.trace.id"

  var appId: String = ""
  var appName: String = ""
  var attemptId: String = ""
  var submitHost: String = ""
  var queue: String = ""
  var driverHost: String = ""
  var sparkVersion: String = SPARK_VERSION
  var user: String = ""
  var traceId: String = ""

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {

  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

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

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val attribute = applicationStart.driverAttributes.get
    appId = applicationStart.appId.getOrElse("-1")
    appName = applicationStart.appName
    attemptId = applicationStart.appAttemptId.getOrElse("0")
    submitHost = attribute.getOrElse("spark.submit.host", "")
    queue = attribute.getOrElse("spark.yarn.queue", "")
    driverHost = attribute.getOrElse("spark.driver.host", "")
    user = applicationStart.sparkUser
    traceId = attribute.getOrElse("spark.trace.id", "")
    val startTime: Long = applicationStart.time
    KafkaProducerUtil.report(new ApplicationDataRecord(
      appId,
      appName,
      attemptId,
      submitHost,
      queue,
      "",
      startTime = startTime,
      driverHost = driverHost,
      status = "RUNNING",
      user = user,
      sparkVersion = sparkVersion,
      traceId = traceId))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val endTime: Long = applicationEnd.time
    KafkaProducerUtil.report(new ApplicationDataRecord(
      appId,
      appName,
      attemptId,
      submitHost,
      queue,
      "",
      endTime = endTime,
      driverHost = driverHost,
      status = "END",
      user = user,
      sparkVersion = sparkVersion,
      traceId = traceId))
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
          user: String,
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
    case _ => // Ignore
  }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {

  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val SparkListenerSQLExecutionStart(executionId, description, details,
    physicalPlanDescription, sparkPlanInfo, time, properties, sqlText) = event
    val user = if (sys.props("user.name").nonEmpty) sys.props("user.name") else ""
    KafkaProducerUtil.report(new ExecutionDataRecord(
      appId,
      attemptId,
      traceId,
      user,
      executionId,
      "RUNNING",
      sqlText,
      details,
      physicalPlanDescription,
      time,
      0L
    ))
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val executionId = event.executionId
    val time = event.time
    val user = if (sys.props("user.name").nonEmpty) sys.props("user.name") else ""
    KafkaProducerUtil.report(new ExecutionDataRecord(
      appId,
      attemptId,
      traceId,
      "",
      executionId,
      "END",
      "",
      "",
      "",
      0L,
      time
    ))
  }
}
