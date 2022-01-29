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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

abstract class MergeTableBaseCommand extends DataWritingCommand{
  def catalogTable: CatalogTable
  def relation: Option[HadoopFsRelation]

  def isHiveTable: Boolean = {
    catalogTable.provider.isEmpty ||
      (catalogTable.provider.isDefined && "hive".equalsIgnoreCase(catalogTable.provider.get))
  }

  override def outputColumnNames: Seq[String] = query.output.map(_.name)

  override def run(session: SparkSession, child: SparkPlan): Seq[Row] = {
    val command = getWritingCommand(session)
    command.run(session, child)
    Seq.empty
  }

  def getWritingCommand(session: SparkSession): DataWritingCommand
}
