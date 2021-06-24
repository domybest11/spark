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

package org.apache.spark

import java.lang.management.{GarbageCollectorMXBean, ManagementFactory}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.util.StopWatch

import org.apache.spark.JvmPauseMonitor.GcTimes
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.sink.KafkaHttpSink
import org.apache.spark.status.JvmPausedWrap

private[spark] class JvmPauseMonitor(sparkConf: SparkConf, appId: String,
                                     appAttemptId: Int, executorId: String
                                    ) extends Logging{

  private val SLEEP_INTERVAL_MS = 10000

  private val warnThresholdMs: Long = sparkConf.get(JVM_PAUSE_MONITOR_INTERVAL)
  private val url: String = sparkConf.get(METRICS_SINK_LANCER_URL)
  private val logId: String = sparkConf.get(METRICS_SINK_LOG_ID)

  private var numGcWarnThresholdExceeded: Long = _
  private var totalGcExtraSleepTime: Long = _

  private var monitorThread: Thread = _
  private val kafkaHttpSink: KafkaHttpSink = new KafkaHttpSink(url, logId)
  private var shouldRun: Boolean = true

  def start(): Unit = {
    kafkaHttpSink.start()
    if (monitorThread != null) {
      throw new IllegalStateException("Already started")
    }
    val task = new Runnable {
      override def run(): Unit = {
        val sw = new StopWatch
        var gcTimesBeforeSleep: mutable.HashMap[String, GcTimes] = getGcTimes
        logInfo("Starting JVM pause monitor")
        while (shouldRun) {
          sw.reset().start()
          try {
            Thread.sleep(SLEEP_INTERVAL_MS)
          } catch {
            case e: InterruptedException => return
          }
          val extraSleepTime = sw.now(TimeUnit.MILLISECONDS) - SLEEP_INTERVAL_MS
          val gcTimesAfterSleep: mutable.HashMap[String, GcTimes] = getGcTimes

          if (extraSleepTime > warnThresholdMs) {
            numGcWarnThresholdExceeded += 1
            logWarning(formatMessage(extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep))
            val jvmPausedWrap = new JvmPausedWrap(appId, appAttemptId, executorId,
              extraSleepTime, System.currentTimeMillis())
            kafkaHttpSink.produce(jvmPausedWrap)
          }
          totalGcExtraSleepTime += extraSleepTime
          gcTimesBeforeSleep = gcTimesAfterSleep
        }
      }
    }
    monitorThread = new Thread(task)
    if (shouldRun) {
      monitorThread.setName("JvmPauseMonitor")
      monitorThread.setDaemon(true)
      monitorThread.start()
    } else {
      logWarning("stop() was called before start() completed")
    }
  }

  def stop(): Unit = {
    kafkaHttpSink.stop()
    shouldRun = false
    if (isStarted) {
      monitorThread.interrupt()
      try {
        monitorThread.join()
      } catch {
        case e: InterruptedException => Thread.currentThread().interrupt()
      }
    }
  }

  def isStarted: Boolean = {
    monitorThread != null
  }

  def getNumGcWarnThresholdExceeded: Long = {
    numGcWarnThresholdExceeded
  }

  def getTotalGcExtraSleepTime: Long = {
    totalGcExtraSleepTime
  }

  def formatMessage(extraSleepTime: Long,
                    gcTimesAfterSleep: mutable.HashMap[String, GcTimes],
                    gcTimesBeforeSleep: mutable.HashMap[String, GcTimes]): String = {
    val gcBeanNames = gcTimesAfterSleep.keySet.intersect(gcTimesBeforeSleep.keySet).seq
    val gcDiffs = ListBuffer[String]()
    for( name <- gcBeanNames) {
      val diff = gcTimesAfterSleep(name).subtract(gcTimesBeforeSleep(name))
      if (diff.gcCount != 0) {
        gcDiffs += ("GC pool '" + name + "' had collection(s): " + diff.toString)
      }
    }

    var ret = "Detected pause in JVM or host machine (eg GC): " + "pause of approximately " +
      extraSleepTime + "ms\n"
    if (gcDiffs.isEmpty) {
      ret += "No GCs detected"
    } else {
      ret += gcDiffs.mkString("\n")
    }
    ret
  }

  def getGcTimes: mutable.HashMap[String, GcTimes] = {
    val map = new mutable.HashMap[String, GcTimes]
    val gcBeans: java.util.List[GarbageCollectorMXBean] =
      ManagementFactory.getGarbageCollectorMXBeans

    gcBeans.asScala.foreach((gcBean: GarbageCollectorMXBean) =>
      map.put(gcBean.getName, new JvmPauseMonitor.GcTimes(gcBean)))
    map
  }
}

object JvmPauseMonitor {
  class GcTimes {
    var gcCount: Long = _
    var gcTimeMillis: Long = _
    def this(gcBean: GarbageCollectorMXBean) {
      this
      gcCount = gcBean.getCollectionCount
      gcTimeMillis = gcBean.getCollectionTime
    }

    def this(count: Long, time: Long) {
      this
      this.gcCount = count
      this.gcTimeMillis = time
    }

    def subtract(other: GcTimes): GcTimes = new GcTimes(this.gcCount - other.gcCount,
      this.gcTimeMillis - other.gcTimeMillis)
  }
}
