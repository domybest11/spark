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

package org.apache.spark.util

import org.json4s._
import org.json4s.jackson.Serialization

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.internal.config.Status._

private[spark] class TraceReporter(conf: SparkConf) {

  private val enableTraceReporter = conf.get(TRACE_REPORTER_ENABLED)

  private val app = conf.get(TRACE_REPORTER_APP)

  private val source = conf.get(TRACE_REPORTER_SOURCE)

  private val logId = conf.get(TRACE_REPORTER_LOG_ID)

  private val lancerUrl = conf.get(TRACE_REPORTER_LANCER_URL)

  implicit val formats = DefaultFormats

  def postProcess(traceId: Option[String], action: String, parentEvent: String,
                status: JobExecutionStatus, message: String, triggerTime: Long): Unit = {
    val recordStatus = status match {
      case JobExecutionStatus.RUNNING => 1
      case JobExecutionStatus.SUCCEEDED => 2
      case _ => 3
    }
    val params = Map[String, Any]("recordAction" -> action, "recordStatus" -> recordStatus,
      "parentRecordAction" -> parentEvent, "recordType" -> 1, "message" -> message,
      "triggerTime" -> triggerTime)
    post(traceId, params)
  }

  def postEvent(traceId: Option[String], action: String, parentEvent: String,
                message: String, triggerTime: Long): Unit = {
    postEvent(traceId, action, parentEvent, message, null, triggerTime)
  }

  def postEvent(traceId: Option[String], action: String, parentEvent: String,
                message: String, tags: Map[String, Any], triggerTime: Long): Unit = {
    val params = Map[String, Any]("recordAction" -> action, "recordStatus" -> 2,
      "parentRecordAction" -> parentEvent, "recordType" -> 2, "message" -> message, "tags" -> tags,
      "triggerTime" -> triggerTime)
    post(traceId, params)
  }

  private def post(traceId: Option[String], params: Map[String, Any]): Unit = {
    val finalTraceId = traceId match {
      case Some(_) => traceId
      case None => conf.getOption("spark.trace.id")
    }
    if (finalTraceId.isDefined && enableTraceReporter && Option(lancerUrl).isDefined) {
      val triggerTime = System.currentTimeMillis()
      val commonParams = Map("app" -> app, "recordSource" -> source, "traceId" -> finalTraceId.get)
      val paramJson = Serialization.write(commonParams ++ params)
      HttpClientUtils.getInstance().doPost(s"$logId$triggerTime$paramJson", lancerUrl)
    }
  }
}

private[spark] object TraceReporter {

  object Msg extends Enumeration {
    type Msg = Value
    val COMPILE = Value("计算引擎解析")
    val EXECUTE = Value("计算引擎执行")
  }

  def createTraceReporter(conf: SparkConf): TraceReporter = {
    new TraceReporter(conf)
  }

  def createTraceReporter(conf: Map[String, String]): TraceReporter = {
    val sparkConf = new SparkConf
    sparkConf.setAll(conf)
    new TraceReporter(sparkConf)
  }
}
