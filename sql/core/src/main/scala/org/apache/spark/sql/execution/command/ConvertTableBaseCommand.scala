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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.LongType

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

  val staticPartitions = {
    if (catalogTable.partitionColumnNames.nonEmpty) {
      val map = scala.collection.mutable.Map[String, String]()
      val firstPartitionName = catalogTable.partitionColumnNames.head
      updatePartitions.foreach{
        partition =>
          val value = partition.spec(firstPartitionName)
          map += value -> firstPartitionName
      }
      if (map.size == 1) {
        val (value, key) = map.head
        Map[String, String](key -> value)
      } else {
        Map.empty[String, String]
      }
    } else {
      Map.empty[String, String]
    }
  }

  override def outputColumnNames: Seq[String] = output.map(_.name)

  override val output: Seq[Attribute] = {
    AttributeReference("origin", LongType, nullable = false)() ::
      AttributeReference("current", LongType, nullable = false)() :: Nil
  }

  override def run(session: SparkSession, child: SparkPlan): Seq[Row] = {
    val command = getWritingCommand(session)
    val originSize = if (catalogTable.partitionColumnNames.isEmpty) {
      catalogTable.properties.getOrElse("totalSize", "0").toLong
    } else {
      updatePartitions.map(p => p.parameters.getOrElse("totalSize", "0").toLong).sum
    }
    if (session.sessionState.conf.tableMetaConvert) {
      session.sessionState.catalog.alterTable(catalogTable)
    }
    command.run(session, child)

    val curSize = if (catalogTable.partitionColumnNames.isEmpty) {
      session.sessionState.catalog.getTableMetadata(catalogTable.identifier)
        .properties.getOrElse("totalSize", "0").toLong
    } else {
      val partitionCompressKey = catalogTable.storage.outputFormat.getOrElse("")
        .toLowerCase(Locale.ROOT) match {
        case fileFormat if fileFormat.endsWith("parquetoutputformat") =>
          "parquet.compression"
        case fileFormat if fileFormat.endsWith("orcoutputformat") =>
          "orc.compress"
        case _ => ""
      }

      var tempRet = 0L
      val refreshPartitions = updatePartitions.map{ partition =>
        val partitionInfo = session.sessionState.catalog.getPartition(
          catalogTable.identifier, partition.spec)

        val map: Map[String, String] = if ("".equals(partitionCompressKey)) {
          partitionInfo.parameters
        } else {
          if (catalogTable.properties.contains(partitionCompressKey)) {
            partitionInfo.parameters +
              (partitionCompressKey -> catalogTable.properties(partitionCompressKey))
          } else {
            partitionInfo.parameters
          }
        }
        tempRet += partitionInfo.parameters.getOrElse("totalSize", "0").toLong
        partitionInfo.copy(storage = partition.storage, parameters = map)
      }

      if (refreshPartitions.nonEmpty) {
        session.sessionState.catalog.alterPartitions(catalogTable.identifier, refreshPartitions)
      }
      tempRet
    }

    Seq(Row(originSize, curSize))
  }

  def getWritingCommand(session: SparkSession): DataWritingCommand

}
