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

import org.apache.spark.internal.Logging
import org.apache.spark.util.TidbConnection.getInstance


class OprationImpl extends TidbOpration with Logging {

  def apply(adhoc: CallChain.AdhocRespository, stype: String) = {
    insertSqlOrPath(adhoc, stype)
  }

  def apply(adhoc: CallChain.AdhocRespository, content: String, boolean: Boolean): Unit = {
    insertBloodLineage(adhoc, content)
  }

  def apply(etl: CallChain.EtlRespository, content: String, boolean: Boolean): Unit = {
    insertBloodLineage(etl, content)
  }

  def apply(etl: CallChain.EtlRespository, stype: String) = {
    insertSqlOrPath(etl, stype)
  }

  override def insertSqlOrPath(adhoc: CallChain.AdhocRespository, stype: String): Unit = {
    val insertSql = "insert into " + getTable(stype) + "(" + getField(stype) + ", projectHistoryId, jobHistoryId, " +
      "sparkAppName) values (?, ?, ?, ?)"
    try {
      val statement = getInstance().prepareStatement(insertSql)
      statement.setQueryTimeout(5)
      statement.setString(1, adhoc.runSql)
      statement.setObject(2, adhoc.projectHistoryId)
      statement.setObject(3, adhoc.jobHistoryId)
      statement.setObject(4, adhoc.sparkAppName)
      statement.execute
    } catch {
      case e: Exception =>
        logError("insert database hdfs sql error ", e)
    }
  }

  override def insertSqlOrPath(etl: CallChain.EtlRespository, stype: String): Unit = {

    val insertSql = "insert into " + getTable(stype) + "(" + getField(stype) + ", projectId, jobId, projectHistoryId," +
      " busTime, " +
      "jobHistoryId, sparkAppName) values (?, ?, ?, ?, ?, ?, ?)"
    try {
      val statement = getInstance().prepareStatement(insertSql)
      statement.setQueryTimeout(5)
      statement.setString(1, etl.runSql)
      statement.setObject(2, etl.projectId)
      statement.setObject(3, etl.jobId)
      statement.setObject(4, etl.projectHistoryId)
      statement.setObject(5, etl.busTime)
      statement.setObject(6, etl.jobHistoryId)
      statement.setObject(7, etl.sparkAppName)
      statement.execute
    } catch {
      case e: Exception =>
        logError("insert database hdfs sql error ", e)
    }
  }


  override def insertBloodLineage(etl: CallChain.EtlRespository, content: String): Unit = {

    val insertSql = "insert into spark_task_bloodlineage (fromtable, totable, projectId, jobId, projectHistoryId," +
      " busTime, " +
      "jobHistoryId, sparkAppName) values (?, ?, ?, ?, ?, ?, ?, ?)"
    try {
      val statement = getInstance().prepareStatement(insertSql)
      statement.setQueryTimeout(5)
      val middle = content.indexOf("#")
      statement.setString(1, content.substring(0, middle))
      statement.setString(2, content.substring(middle + 1, content.length))
      statement.setObject(3, etl.projectId)
      statement.setObject(4, etl.jobId)
      statement.setObject(5, etl.projectHistoryId)
      statement.setObject(6, etl.busTime)
      statement.setObject(7, etl.jobHistoryId)
      statement.setObject(8, etl.sparkAppName)
      statement.execute
    } catch {
      case e: Exception =>
        logError("insert database hdfs sql error ", e)
    }
  }

  override def insertBloodLineage(adhoc: CallChain.AdhocRespository, content: String): Unit = {
    val insertSql = "insert into spark_task_bloodlineage (fromtable, totable, projectHistoryId, jobHistoryId, " +
      "sparkAppName) values (?, ?, ?, ?, ?)"
    try {
      val statement = getInstance().prepareStatement(insertSql)
      statement.setQueryTimeout(5)
      val middle = content.indexOf("#")
      statement.setString(1, content.substring(0, middle))
      statement.setObject(2, content.substring(middle + 1, content.length))
      statement.setObject(3, adhoc.projectHistoryId)
      statement.setObject(4, adhoc.jobHistoryId)
      statement.setObject(5, adhoc.sparkAppName)
      statement.execute
    } catch {
      case e: Exception =>
        logError("insert database hdfs sql error ", e)
    }
  }

  private def getField(stype: String): String = {
    var field = ""
    stype match {
      case "sql" => field = "runSql"
      case "path" => field = "runPath"
      case _ => logError(s"nothing matched type {$stype}")
    }
    field
  }

  private def getTable(stype: String): String = {
    var table = ""
    stype match {
      case "sql" => table = "spark_task_sql"
      case "path" => table = "spark_task_path"
      case _ => logError(s"nothing matched type {$stype}")
    }
    table
  }

}
