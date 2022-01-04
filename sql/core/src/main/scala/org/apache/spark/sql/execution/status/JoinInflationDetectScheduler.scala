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

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{LiveExecutionData, SparkPlanGraphNode, SparkPlanGraphWrapper, SQLAppStatusListener}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.{ThreadUtils, TraceReporter}

class JoinInflationDetectScheduler extends Logging {

  var initDelayTime: Long = _
  var periodTime: Long = _
  var inflationFactor: Double = _
  var checkJoinOutputRowsThreshold: Long = _
  var executionShouldCheckTime: Long = _
  var reporter: TraceReporter = _
  val action = "joinDetect"
  val alreadyReportExecutors = new ConcurrentHashMap[Long, LiveExecutionData]()
  val joinDetectScheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("join-inflation-detect-thread")

  def init(conf: SparkConf): Unit = {
    initDelayTime = conf.getLong("spark.sql.join.inflation.detect.delay.time", 20)
    periodTime = conf.getLong("spark.sql.join.inflation.detect.period.time", 10)
    inflationFactor = conf.getDouble("spark.sql.join.inflation.detect.factor", 10.0)
    checkJoinOutputRowsThreshold =
      conf.getLong("spark.sql.join.inflation.detect.threshold", 1000000)
    executionShouldCheckTime =
      conf.getLong("spark.sql.join.inflation.should.check.time", 10 * 60 * 1000)
    reporter = TraceReporter.createTraceReporter(conf)
  }
  def start(conf: SparkConf, kvStore: ElementTrackingStore,
            listener: SQLAppStatusListener): Unit = {
    init(conf)
    joinDetectScheduler.scheduleWithFixedDelay(new Runnable() {
      override def run(): Unit = {
        try {
          joinDetect(kvStore, listener)
        } catch {
          case e: Exception =>
            log.warn("Ignore join detect failed", e)
        }
      }
    }, initDelayTime, periodTime, TimeUnit.MINUTES)

  }

  def joinDetect(store: ElementTrackingStore, listener: SQLAppStatusListener): Unit = {
    val liveExecutions = listener.getLiveExecutions
    // clean already finished executions from alreadyReportExecutors
    alreadyReportExecutors.keySet().asScala.foreach(k => {
      if (!liveExecutions.containsKey(k)) {
        alreadyReportExecutors.remove(k)
      }
    })
    liveExecutions.entrySet().asScala.foreach(l => {
      if (!alreadyReportExecutors.containsKey(l.getKey) && shouldCheck(l.getValue)) {
        val executionId = l.getValue.executionId
        val traceId = l.getValue.traceId
        val ignored = l.getValue.ignored
        val executionMetrics = listener.liveExecutionMetrics(executionId)
        val graphWrapper = store.read(classOf[SparkPlanGraphWrapper], executionId)
        val joinNodes = graphWrapper.nodes.filter(n => {
          n.node != null && (n.node.name.endsWith("Join") || n.node.name.equals("CartesianProduct"))
        })
        if (joinNodes.nonEmpty) {
          joinNodes.foreach(j => {
            // output rows for join node
            val joinNodeOutputRows = getMetricsFromNode(j.node, graphWrapper, executionMetrics, getFromParent = false)
            if (joinNodeOutputRows.isDefined
              && joinNodeOutputRows.get > checkJoinOutputRowsThreshold) {
              val parentNodes = getParentNode(j.node, graphWrapper)
              val parentOutputRows = parentNodes.map(p => {
                getMetricsFromNode(p, graphWrapper, executionMetrics, getFromParent = true)
              })
              val maxParentOutputRows = parentOutputRows.map(_.getOrElse(0L)).max
              // check inflation
              if (!ignored && maxParentOutputRows > 0
                && joinNodeOutputRows.get >= maxParentOutputRows * inflationFactor) {
                // scalastyle:off
                val postMsg = s"注意: ${j.node.desc} 膨胀超过了 ${inflationFactor} 倍"
                reporter.postEvent(traceId, action, "", postMsg, System.currentTimeMillis())
                alreadyReportExecutors.putIfAbsent(l.getKey, l.getValue)
              }
            }
          })
        }
      }
    })
  }

  def getMetricsFromNode(node: SparkPlanGraphNode, graphWrapper: SparkPlanGraphWrapper,
                         executionMetrics: Option[Map[Long, String]], getFromParent: Boolean): Option[Long] = {
    if (executionMetrics.isEmpty) {
      None
    } else {
      val metrics = node.metrics.filter(_.name.equals("number of output rows"))
      // if metrics is empty, use metrics of parent node
      if (metrics.isEmpty && getFromParent) {
        val nodes = getParentNode(node, graphWrapper)
        if (nodes.size == 1) {
          getMetricsFromNode(nodes.head, graphWrapper, executionMetrics, getFromParent)
        } else {
          None
        }
      } else {
        val accumulatorId = metrics.head.accumulatorId
        if (executionMetrics.get.contains(accumulatorId)) {
          Option(executionMetrics.get(accumulatorId).toLong)
        } else {
          None
        }
      }
    }
  }

  def getParentNode(node: SparkPlanGraphNode, graphWrapper: SparkPlanGraphWrapper): Seq[SparkPlanGraphNode] = {
    val edges = graphWrapper.edges.filter(_.toId == node.id)
    edges.flatMap(e => {
      graphWrapper.nodes.filter(_.node.id == e.fromId).map(_.node)
    })
  }

  def shouldCheck(liveExecutionData: LiveExecutionData): Boolean = {
    System.currentTimeMillis() - liveExecutionData.submissionTime > executionShouldCheckTime
  }
}
