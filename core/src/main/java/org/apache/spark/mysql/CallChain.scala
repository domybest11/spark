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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

object CallChain extends Logging {

  trait Handler {
    def handler(event: Event)
  }

  class DefaultHandler extends Handler {
    override def handler(event: Event): Unit = {
      logInfo("default handler handle")
    }
  }

  trait EtlHandler extends Handler {
    abstract override def handler(event: Event): Unit = {
      try {
        val name = event.appName
        if (StringUtils.isNotBlank(name) && (name.startsWith("a_s_") || name.startsWith("a_h_"))) {
          val points = name.split("_")
          event.sqlOrPath match {
            case "bloodlineage" => InsertOpration(event.content, new EtlRespository(event.content, points(2), points(3), points
            (4), points(5)
              , points(6), name), true)
            case _ => InsertOpration(event.sqlOrPath, new EtlRespository(event.content, points(2), points(3), points
            (4), points(5)
              , points(6), name))
          }
        } else {
          super.handler(event)
        }
      } catch {
        case e: Exception => logError("handler error ", e)
      }
    }
  }

  trait AdHocHandler extends Handler {
    abstract override def handler(event: Event): Unit = {
      try {
        val name = event.appName
        if (event.appName.startsWith("a_h_q_")) {
          val points = name.split("_")
          event.sqlOrPath match {
            case "bloodlineage" => InsertOpration(event.content, AdhocRespository(event.content, points(3), points(4)
              , name), true)
            case _ => InsertOpration(event.sqlOrPath, AdhocRespository(event.content, points(3), points(4), name))
          }
        } else {
          super.handler(event)
        }
      } catch {
        case e: Exception => logError("handler error ", e)
      }
    }
  }

  case class Event(content: String, appName: String, sqlOrPath: String)

  case class EtlRespository(runSql: String, projectId: String, jobId: String, projectHistoryId: String, busTime: String, jobHistoryId: String, sparkAppName: String)

  case class AdhocRespository(runSql: String, projectHistoryId: String, jobHistoryId: String, sparkAppName: String)

  val handler = new DefaultHandler with EtlHandler with AdHocHandler



}
