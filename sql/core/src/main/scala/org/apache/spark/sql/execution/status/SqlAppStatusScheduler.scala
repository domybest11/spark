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

import java.util.concurrent.{Executors, LinkedBlockingQueue, ScheduledExecutorService, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SQLExecutionUIData
import org.apache.spark.util.ShutdownHookManager


object SqlAppStatusScheduler{
  val POISON_PILL = new SQLExecutionUIData(-1, "", "", "",
    Seq.empty, 0L, None, Map.empty, Set.empty, Map.empty)
}

class SqlAppStatusScheduler(sparkContext: SparkContext) extends Logging{
  import SqlAppStatusScheduler.POISON_PILL
  private var kafkaSink: SqlAppStoreKafkaProducer = _
  private var appStatusStore: SqlAppStoreStatusStoreV1 = _
  private var sinkService: ScheduledExecutorService = _
  private val MILLISECONDS_ONE_MINUTE: Int = 1000 * 60
  private val EXECUTION_INFO_QUEUE_CAPACITY: Int = 10000
  private var _shutdownHookRef: AnyRef = _
  val _executionInfoQueue =
    new LinkedBlockingQueue[SQLExecutionUIData](EXECUTION_INFO_QUEUE_CAPACITY)
  val sendOnceMetricKafkaThread = new Thread(s"sql-sink-once-metric-task") {
    setDaemon(true)
    override def run(): Unit = {
      var next: SQLExecutionUIData = _executionInfoQueue.take()
      while (next != POISON_PILL) {
        sendOnceKafka(next)
        next = _executionInfoQueue.take()
      }
    }
  }

  def init(conf: SparkConf): Unit = {
    kafkaSink = new SqlAppStoreKafkaProducer(conf)
    sinkService = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("sql-sink-metric-task-periodically-%d").build)
  }

  def start(appStatusStore: SqlAppStoreStatusStoreV1): Unit = {
    this.appStatusStore = appStatusStore
    init(appStatusStore.conf)
    sinkService.scheduleAtFixedRate(new Runnable {
      override def run() =
        try {
          sendPeriodlyKafka
        } catch {
          case e: Exception =>
            logWarning(s"send sql metric to kafka periodically occurred errors: ${e.getMessage}")
        }
    }, MILLISECONDS_ONE_MINUTE, MILLISECONDS_ONE_MINUTE, TimeUnit.MILLISECONDS)
    sendOnceMetricKafkaThread.start()
    _shutdownHookRef = ShutdownHookManager.addShutdownHook{() =>
      logInfo("Invoking stopMetricThread from shutdown hook")
      try {
        stopMetricThread()
      } catch {
        case e: Throwable =>
          logWarning(s"Ignoring Exception while stopping " +
            s"SqlAppStatusScheduler from shutdown hook ${e.getMessage}")
      }
    }
  }

  def sendPeriodlyKafka(): Unit = {
    logDebug("starting to send sql metrics to Kafka periodically")
    val sqlExecutionList: Seq[SQLExecutionUIData] = appStatusStore.executionsList
      .filterNot(sqlExecution => sqlExecution.statement == null ||
        sqlExecution.statement.equals(""))
    if (null == sqlExecutionList || sqlExecutionList.isEmpty) {
      return
    }
    sqlExecutionList.filter(!_.finishAndReport).foreach(executionInfo => {
      logDebug("starting to assemble executionInfo from sqlAppStatusStore")
      var applicationSQLExecutionData: ApplicationSQLExecutionData = null
      try {
        applicationSQLExecutionData = appStatusStore
          .assembleExecutionInfo(executionInfo, "periodly").get
      } catch {
        case e: Exception =>
          logWarning(s"assemble the sqlAppStatusStore occurred errors: ${e.getMessage}")
          return
      }
      try {
        val reportWrap: SqlAppStoreReportWrap =
          new SqlAppStoreReportWrap(applicationSQLExecutionData.appId,
            applicationSQLExecutionData.attemptId,
            applicationSQLExecutionData.role,
            applicationSQLExecutionData.service,
            applicationSQLExecutionData.user,
            "METRICS",
            applicationSQLExecutionData.host,
            applicationSQLExecutionData.currentTime,
            applicationSQLExecutionData)
        kafkaSink.report(reportWrap)
      } catch {
        case e: Exception =>
          logWarning(s"send sql metrics to Kafka periodically occurred errors: ${e.getMessage}")
          return
      }
      logDebug("ended to send sql metrics to Kafka periodically")
    })
  }

  def sendOnceKafka(executionInfo: SQLExecutionUIData): Unit = {
    logDebug("starting to send an sql metrics to Kafka")
    if (executionInfo.statement == null || executionInfo.statement.equals("")) {
      return
    }
    if (null == executionInfo) {
      return
    }
    logDebug("starting to assemble an executionInfo from sqlAppStatusStore")
    var applicationSQLExecutionData: ApplicationSQLExecutionData = null
    try {
      applicationSQLExecutionData = appStatusStore
        .assembleExecutionInfo(executionInfo, "once").get
    } catch {
      case e: Exception =>
        logWarning(s"assemble the sql executionInfo occurred errors: ${e.getMessage}")
        return
    }
    try {
      val reportWrap: SqlAppStoreReportWrap =
        new SqlAppStoreReportWrap(applicationSQLExecutionData.appId,
          applicationSQLExecutionData.attemptId,
          applicationSQLExecutionData.role,
          applicationSQLExecutionData.service,
          applicationSQLExecutionData.user,
          "METRICS",
          applicationSQLExecutionData.host,
          applicationSQLExecutionData.currentTime,
          applicationSQLExecutionData)
        kafkaSink.report(reportWrap)
    } catch {
      case e: Exception =>
        logWarning(s"send an sql metrics to Kafka occurred errors: ${e.getMessage}")
        return
    }
    logDebug("ended to send an sql metrics to Kafka")
  }

  def stopMetricThread(): Unit = {
    _executionInfoQueue.put(POISON_PILL)
    if (Thread.currentThread() != sendOnceMetricKafkaThread) {
      sendOnceMetricKafkaThread.join()
    }
    sinkService.shutdown()
    if (_shutdownHookRef != null) {
      ShutdownHookManager.removeShutdownHook(_shutdownHookRef)
    }
    logInfo("sql metric thread is stoped.")
  }
}


