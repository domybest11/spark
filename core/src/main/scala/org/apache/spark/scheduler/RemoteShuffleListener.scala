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

package org.apache.spark.scheduler

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_REMOTE_REPORT_INTERVAL
import org.apache.spark.remote.shuffle.RemoteShuffleClient

private[spark] class RemoteShuffleListener(
    appId: String,
    appAttemptId : Option[String],
    sparkConf: SparkConf,
    shuffleClient: RemoteShuffleClient)
  extends SparkListener with Logging {

  private val reportInterval = sparkConf.get(SHUFFLE_REMOTE_REPORT_INTERVAL)

  def start(): Unit = {
    shuffleClient.start()
    logInfo(s"remote shuffle service driver client start success")
  }

  def stop(): Unit = {
    shuffleClient.stop()
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    shuffleClient.startApplication(appId, appAttemptId, reportInterval)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    shuffleClient.stopApplication(appId, appAttemptId)
  }
}
