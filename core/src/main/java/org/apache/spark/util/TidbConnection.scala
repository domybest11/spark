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

import java.sql.{Connection, DriverManager}

import org.apache.spark.internal.Logging


object TidbConnection extends Logging {


  private var connection: Connection = null

  def getInstance(): Connection = {
    if (connection == null) {
      connection = new TidbConnection().getTidbConnection()
    }
    connection
  }

}

class TidbConnection extends Logging {

  private val url = "elephant_mysql_url"
  private val user = "elephant_mysql_user"
  private val passwd = "elephant_mysql_passwd"

  private def getTidbConnection(): Connection = {
    try {
      this.synchronized {
        if (TidbConnection.connection == null) {
          checkArg(List(getValue(url), getValue(user), getValue(passwd)))
          Class.forName("com.mysql.jdbc.Driver")
          TidbConnection.connection = DriverManager.getConnection(getValue(url).get, getValue
          (user).get, getValue(passwd).get)
          logInfo("get MySQL connection succeeded!")
        }
      }
      TidbConnection.connection
    } catch {
      case e: Exception =>
        logError("get connection error ", e)
        TidbConnection.connection
    }
  }

  private def getValue(key: String): Option[String] =
    Option(ConfigurationUtil.getSparkCenterConfiguration.getProperty(key))

  private def checkArg(list: List[Option[String]]): Unit = list.map(l => check(l))

  private def check(arg: Option[String]): Unit = {
    arg match {
      case Some(_) =>
      case None => throw new Exception(arg + " is null")
    }
  }

}
