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

import java.util.Locale

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

abstract class ConvertTableBaseCommand extends DataWritingCommand {
  def catalogTable: CatalogTable
  def relation: Option[HadoopFsRelation]
  def fileFormat: Option[String]
  def compressType: Option[String]
  def updatePartitions: Seq[CatalogTablePartition]

  def isHiveTable: Boolean = {
    catalogTable.provider.isEmpty ||
      (catalogTable.provider.isDefined && "hive".equalsIgnoreCase(catalogTable.provider.get))
  }

  override def outputColumnNames: Seq[String] = query.output.map(_.name)

  override def run(session: SparkSession, child: SparkPlan): Seq[Row] = {
    val command = getWritingCommand(session)
    command.run(session, child)
    session.sessionState.catalog.alterTable(catalogTable)
    val partitionCompressKey = catalogTable.storage.outputFormat.getOrElse("")
      .toLowerCase(Locale.ROOT) match {
      case fileFormat if fileFormat.endsWith("parquetoutputformat") =>
        "parquet.compression"
      case fileFormat if fileFormat.endsWith("orcoutputformat") =>
        "orc.compress"
      case _ => ""
    }
    val refreshPartitions = updatePartitions.map{ partition =>
      val partitionInfo = session.sessionState.catalog.getPartition(
        catalogTable.identifier, partition.spec)
      val map: Map[String, String] = if ("".equals(partitionCompressKey)) {
        partitionInfo.parameters
      } else {
        partitionInfo.parameters +
          (partitionCompressKey -> catalogTable.properties(partitionCompressKey))
      }
      partitionInfo.copy(storage = partition.storage, parameters = map)
    }
    if (refreshPartitions.nonEmpty) {
      session.sessionState.catalog.alterPartitions(catalogTable.identifier, refreshPartitions)
    }
    Seq.empty
  }

  def getWritingCommand(session: SparkSession): DataWritingCommand

}
