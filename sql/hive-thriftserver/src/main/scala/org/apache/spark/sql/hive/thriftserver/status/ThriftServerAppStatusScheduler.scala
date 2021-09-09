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

package org.apache.spark.sql.hive.thriftserver.status

import java.util.concurrent.{Executors, LinkedBlockingQueue, ScheduledExecutorService, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.hive.thriftserver.ui.{HiveThriftServer2Listener, LiveExecutionData, SessionStatus}



object ThriftServerAppStatusScheduler{
  val POISON_PILL = new LiveExecutionData("", "", "", 0, "")
}

class ThriftServerAppStatusScheduler extends Logging{
  import ThriftServerAppStatusScheduler.POISON_PILL
  private var kafkaSink: ThriftServerKafkaProducer = _
  var listener: HiveThriftServer2Listener = HiveThriftServer2.listener
  private var appStatusStore: ThriftServerSqlAppStatusStore = _
  var sinkService: ScheduledExecutorService = _
  private val MILLISECONDS_ONE_MINUTE: Int = 1000 * 60
  private val EXECUTION_INFO_QUEUE_CAPACITY: Int = 10000
  val _executionInfoQueue =
    new LinkedBlockingQueue[LiveExecutionData](EXECUTION_INFO_QUEUE_CAPACITY)
  val sendOnceMetricKafkaThread = new Thread("sink-once-metric-task") {
    setDaemon(true)
    override def run(): Unit = {
      var next: LiveExecutionData = _executionInfoQueue.take()
      while (next != POISON_PILL) {
        sendOnceKafka(next)
        next = _executionInfoQueue.take()
      }
    }
  }

   def setThriftServerListener(thriftServerlistener: HiveThriftServer2Listener): Unit = {
     listener = thriftServerlistener
   }

   def init(conf: SparkConf): Unit = {
    kafkaSink = new ThriftServerKafkaProducer(conf)
    sinkService = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("sink-metric-task-periodically-%d").build)
  }

  def start(appStatusStoreV1: ThriftServerSqlAppStatusStore): Unit = {
    this.appStatusStore = appStatusStoreV1
    init(appStatusStoreV1.conf)
    sinkService.scheduleAtFixedRate(new Runnable {
      override def run() =
        try {
          sendPeriodlyKafka
        } catch {
          case e: Exception =>
            logWarning(s"send Kafka periodically occurred errors: ${e.getMessage}")
        }
    }, MILLISECONDS_ONE_MINUTE, MILLISECONDS_ONE_MINUTE, TimeUnit.MILLISECONDS)
    sendOnceMetricKafkaThread.start()
  }

   def sendPeriodlyKafka(): Unit = {
     logDebug("starting to send metrics to Kafka periodically")
    val sqlExecutionList: Seq[LiveExecutionData] = listener.getExecutionList
    if (null == sqlExecutionList || sqlExecutionList.isEmpty) {
      return
    }
    sqlExecutionList.filter(!_.finishOrErroAndReport).foreach(executionInfo => {
      logDebug("starting to assemble executionInfo from appStatusStore")
      var applicationSQLExecutionData: ApplicationSQLExecutionData = null
      try {
        executionInfo.statementType = executionInfo.appName
        applicationSQLExecutionData = appStatusStore.assembleExecutionInfo(executionInfo).get
      } catch {
        case e: Exception =>
          logWarning(s"assemble the executionInfo occurred errors: ${e.getMessage}")
          return
      }
      try {
        val reportWrap: ThriftServerReportWrap =
          new ThriftServerReportWrap(applicationSQLExecutionData.appId,
          applicationSQLExecutionData.attemptId, applicationSQLExecutionData.role,
          applicationSQLExecutionData.service, applicationSQLExecutionData.user,
          "METRICS",
          applicationSQLExecutionData.host,
          applicationSQLExecutionData.currentTime,
          applicationSQLExecutionData)
        kafkaSink.report(reportWrap)
      } catch {
        case e: Exception =>
          logWarning(s"send metrics to Kafka periodically occurred errors: ${e.getMessage}")
          return
      }
      logDebug("ended to send metrics to Kafka periodically")
    })
  }

   def sendOnceKafka(liveExecutionData: LiveExecutionData): ThriftServerReportWrap = {
   logDebug("starting to send an metrics to Kafka")
     var reportWrap: ThriftServerReportWrap = null
    if (null == liveExecutionData ) {
      return reportWrap
    }
     logDebug("starting to assemble an executionInfo from appStatusStore")
      var applicationSQLExecutionData: ApplicationSQLExecutionData = null
      try {
        applicationSQLExecutionData = appStatusStore.assembleExecutionInfo(liveExecutionData).get
      } catch {
        case e: Exception =>
          logWarning(s"assemble the executionInfo occurred errors: ${e.getMessage}")
          return reportWrap
      }
      try {
          reportWrap =
          new ThriftServerReportWrap(applicationSQLExecutionData.appId,
          applicationSQLExecutionData.attemptId, applicationSQLExecutionData.role,
          applicationSQLExecutionData.service, applicationSQLExecutionData.user,
          "METRICS",
          applicationSQLExecutionData.host,
          applicationSQLExecutionData.currentTime,
          applicationSQLExecutionData)
        if (!liveExecutionData.statement.equals(SessionStatus.SESSION_RUNNING)) {
          kafkaSink.report(reportWrap)
        }
      } catch {
        case e: Exception =>
          logWarning(s"send an metrics to Kafka occurred errors: ${e.getMessage}")
          return reportWrap
      }
     logDebug("ended to send an metrics to Kafka")
     reportWrap
  }
}


