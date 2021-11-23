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

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.{TRACE_REPORTER_APP, TRACE_REPORTER_ENABLED, TRACE_REPORTER_LANCER_URL, TRACE_REPORTER_LOG_ID, TRACE_REPORTER_SOURCE}

/**
 * Created by guxiangrong on 2021/6/29.
 */

private[spark] class AppCostReporter(conf: SparkConf) {

  private val enableTraceReporter = conf.get(TRACE_REPORTER_ENABLED)

  private val lancerUrl = conf.get(TRACE_REPORTER_LANCER_URL)

  private val app = conf.get(TRACE_REPORTER_APP)

  private val source = conf.get(TRACE_REPORTER_SOURCE)

  private val logId = conf.get(TRACE_REPORTER_LOG_ID)

  implicit val formats = DefaultFormats

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

    if (finalTraceId.isDefined && !finalTraceId.get.equals("") &&
      enableTraceReporter && Option(lancerUrl).isDefined) {
      val triggerTime = System.currentTimeMillis()
      val commonParams = Map("app" -> app, "recordSource" -> source, "traceId" -> finalTraceId.get)
      val paramJson = Serialization.write(commonParams ++ params)
      HttpClientUtils.getInstance().doPost(s"$logId$triggerTime$paramJson", lancerUrl)
    }
  }
}

private[spark] object AppCostReporter {
  def createAppCostReporter(conf: SparkConf): AppCostReporter = {
    new AppCostReporter(conf)
  }
}
