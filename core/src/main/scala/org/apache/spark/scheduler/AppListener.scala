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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ApplicationDataRecord, KafkaProducerUtil, Utils}
import org.apache.spark.{SPARK_VERSION, SparkConf}

import scala.collection.mutable.HashMap

private[spark] class AppListener(
    appId: String,
    appAttemptId : Option[String],
    sparkConf: SparkConf)
  extends SparkListener with Logging {

  private val sqlRules = new HashMap[String, Int]()
  private var maxRequestYarnTime = 0L

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    val startTime: Long = event.time
    KafkaProducerUtil.report(new ApplicationDataRecord(
      appId,
      event.appName,
      appAttemptId.getOrElse("0"),
      sparkConf.get("spark.submit.host", ""),
      sparkConf.get("spark.yarn.queue", ""),
      "",
      startTime = startTime,
      driverHost = sparkConf.get("spark.driver.host", ""),
      status = "RUNNING",
      user = event.sparkUser,
      sparkVersion = SPARK_VERSION,
      traceId = sparkConf.get("spark.trace.id", "")))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val endTime: Long = applicationEnd.time
    val HBOEffectiveRules = sparkConf.get("spark.deploy.autoConfEffectiveRules", "")
    if (StringUtils.isNotEmpty(HBOEffectiveRules)) {
      HBOEffectiveRules.split(",").foreach(ruleName => {
        sqlRules.put(ruleName, 1)
      })
    }
    KafkaProducerUtil.report(new ApplicationDataRecord(
      appId,
      sparkConf.get("spark.app.name", ""),
      appAttemptId.getOrElse("0"),
      sparkConf.get("spark.submit.host", ""),
      sparkConf.get("spark.yarn.queue", ""),
      "",
      endTime = endTime,
      driverHost = sparkConf.get("spark.driver.host", ""),
      status = "END",
      user = Utils.getCurrentUserName(),
      sparkVersion = SPARK_VERSION,
      traceId = sparkConf.get("spark.trace.id", ""),
      maxRequestYarnTime = maxRequestYarnTime,
      ruleNames = sqlRules
    ))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerRuleExecute => onRuleExecute(e)
    case e: SparkListenerRequestYarnTime => onYarnRequest(e)
    case _ =>
  }

  def onRuleExecute(event: SparkListenerRuleExecute): Unit = {
    sqlRules.put(event.ruleName, 1)
  }

  def onYarnRequest(event: SparkListenerRequestYarnTime): Unit = {
    maxRequestYarnTime = Math.max(maxRequestYarnTime, event.time)
  }

}


