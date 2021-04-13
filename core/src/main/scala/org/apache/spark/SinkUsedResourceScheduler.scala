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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.KafkaHttpSink
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class SinkUsedResourceScheduler(sinkUsedResource: KafkaHttpSink => Unit,
                                               name: String,
                                               kafkaHttpSink: KafkaHttpSink,
                                               intervalMs: Long) extends Logging {
  private val sinkUsedResourceScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor(name)

  def start(): Unit = {
    kafkaHttpSink.start()
    val sinkUsedResourceSchedulerTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(sinkUsedResource(kafkaHttpSink))
    }
    sinkUsedResourceScheduler.scheduleAtFixedRate(sinkUsedResourceSchedulerTask, intervalMs,
      intervalMs, TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    kafkaHttpSink.stop()
    sinkUsedResourceScheduler.shutdownNow()
    sinkUsedResourceScheduler.awaitTermination(10, TimeUnit.SECONDS)
  }
}
