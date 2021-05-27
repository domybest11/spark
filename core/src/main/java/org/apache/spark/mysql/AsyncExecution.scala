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

package org.apache.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.mysql.CallChain.Event
import org.apache.spark.util.ConfigurationUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}


object AsyncExecution extends Logging {

  def AsycnHandle(event: Event): Unit = {
    val switch = ConfigurationUtil.getSparkCenterConfiguration.getProperty("spark_on_off", "off")
    if (switch.equals("on")) {
      val fun = Future {
        CallChain.handler.handler(event)
      }

      fun onComplete {
        case Success(success) => logInfo(s"asyn running task ${success}")
        case Failure(error) => logError(s"asyn running task ${error}")
        case _ => logError("some Exception")
      }
    }
  }

  def getSparkAppName(conf: SparkConf): String = {
    val appNamePrefix = conf.get(s"spark.app.name.prefix", "")
    val appName = conf.get(s"spark.app.name", "")
    if (appNamePrefix.nonEmpty) {
      appNamePrefix
    } else if (appName.nonEmpty && appName.startsWith("a_h_")) {
      appName
    } else {
      ""
    }
  }

}
