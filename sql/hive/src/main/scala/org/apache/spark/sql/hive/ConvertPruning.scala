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

package org.apache.spark.sql.hive

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.{ConvertStatement, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.HiveSerDe


case class ConvertPruning(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case ConvertStatement(tableRelation, query, fileFormat, compressType, _) =>
        val (pruning, updatePartitions) = rewriteQuery(query, fileFormat, compressType)
        ConvertStatement(tableRelation, pruning, fileFormat, compressType, updatePartitions)
    }
  }

  private def rewriteQuery(
     query: LogicalPlan,
     fileFormat: Option[String],
     compressType: Option[String]): (LogicalPlan, Seq[CatalogTablePartition]) = {
    val catalog = session.sessionState.catalog
    var filter: Option[Filter] = None
    var source: Option[CatalogTable] = None
    query.find {
      case LogicalRelation(_, _, table, _) =>
        source = table
        true
      case r: HiveTableRelation =>
        source = Option(r.tableMeta)
        true
      case f: Filter =>
        filter = Option(f)
        false
      case _ => false
    }

    if (source.isDefined && catalog.tableExists(source.get.identifier)) {
      val targetStorage = fileFormat match {
        case Some(format) =>
          val serde = format.toLowerCase(Locale.ROOT) match {
            case "text" => HiveSerDe.serdeMap("textfile")
            case _ => HiveSerDe.serdeMap(fileFormat.get.toLowerCase(Locale.ROOT))
          }
          Option(serde)
        case None => None
      }

      val sourceSerde = source.get.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
      val compressKey =
        source.get.storage.outputFormat.getOrElse("").toLowerCase(Locale.ROOT) match {
        case fileFormat if fileFormat.endsWith("parquetoutputformat") => "parquet.compression"
        case fileFormat if fileFormat.endsWith("orcoutputformat") => "orc.compress"
        case _ => ""
      }
      val sourceCompress = source.get.properties.getOrElse(compressKey, "").toLowerCase(Locale.ROOT)
      var updatePartitions = Seq.empty[CatalogTablePartition]
      val rewriteQuery = if (source.get.partitionColumnNames.isEmpty) {

        (targetStorage, compressType) match {
          case (None, Some(targetCompress)) =>
            if (targetCompress.toLowerCase(Locale.ROOT)
              .equals(sourceCompress)) {
              throw new AnalysisException("table is meet compressType.")
            } else {
              query
            }
          case (Some(storage), None) =>
            if (storage.serde.get.toLowerCase(Locale.ROOT)
              .equals(sourceSerde)) {
              throw new AnalysisException("table is meet fileFormat.")
            } else {
              query
            }
          case (Some(storage), Some(targetCompress)) =>
            if (storage.serde.get.toLowerCase(Locale.ROOT)
              .equals(sourceSerde)&&
              targetCompress.toLowerCase(Locale.ROOT)
                .equals(sourceCompress)
            ) {
              throw new AnalysisException("table is meet fileFormat and compressType.")
            } else {
              query
            }
          case _ => query
        }
      } else {

        val prunedPartition = catalog.listPartitionsByFilter(
          source.get.identifier, filter.map(f => f.condition).toSeq
        ).filter{
          partition =>
            val partitionSerde = partition.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
            val partitionCompressKey = partition.storage.outputFormat.getOrElse("")
              .toLowerCase(Locale.ROOT) match {
              case fileFormat if fileFormat.endsWith("parquetoutputformat") => "parquet.compression"
              case fileFormat if fileFormat.endsWith("orcoutputformat") => "orc.compress"
              case _ => ""
            }
            val partitionCompress = partition.parameters
              .getOrElse(partitionCompressKey, "").toLowerCase(Locale.ROOT)
            (targetStorage, compressType) match {
              case (None, Some(targetCompress)) =>
                if (partitionCompress.equals(targetCompress.toLowerCase(Locale.ROOT))) {
                  false
                } else {
                  true
                }
              case (Some(storage), None) =>
                if (partitionSerde.equals(storage.serde.get.toLowerCase(Locale.ROOT))) {
                  false
                } else {
                  true
                }
              case (Some(storage), Some(targetCompress)) =>
                if (partitionSerde.equals(storage.serde.get.toLowerCase(Locale.ROOT)) &&
                  partitionCompress.equals(targetCompress.toLowerCase(Locale.ROOT))
                ) {
                  false
                } else {
                  true
                }
              case _ => true
            }
        }.map{
          partition =>
            val storageUpdatePartition = targetStorage match {
              case Some(storage) =>
                val partitionStorage = partition.storage.copy(
                  inputFormat = storage.inputFormat,
                  outputFormat = storage.outputFormat,
                  serde = storage.serde
                )
                partition.copy(storage = partitionStorage)
              case _ => partition
            }
            val updatePartition = compressType match {
              case Some(targetCompress) =>
                val partitionCompressKey = storageUpdatePartition.storage.outputFormat.getOrElse("")
                  .toLowerCase(Locale.ROOT) match {
                  case fileFormat if fileFormat.endsWith("parquetoutputformat") =>
                    "parquet.compression"
                  case fileFormat if fileFormat.endsWith("orcoutputformat") =>
                    "orc.compress"
                  case _ => ""
                }
                val map: Map[String, String] = storageUpdatePartition.parameters +
                  (partitionCompressKey -> targetCompress.toUpperCase(Locale.ROOT))
                storageUpdatePartition.copy(parameters = map)
              case _ => storageUpdatePartition

            }
            updatePartition
        }

        updatePartitions = prunedPartition
        val filterExpression = prunedPartition.map{
          partition =>
            partition.spec.map(entry =>
              EqualTo(UnresolvedAttribute(Seq(entry._1)), Literal(entry._2))
            ).reduce((e1: Expression, e2: Expression) => And(e1, e2))
        }.toArray
        val filterQuery = if (filterExpression.isEmpty) {
          throw new AnalysisException("table is meet fileFormat and compressType.")
        } else {
          val condition = filterExpression
            .reduce((pf1: Expression, pf2: Expression) => Or(pf1, pf2))
          Filter(condition, UnresolvedRelation(source.get.identifier))
        }
        session.sessionState.analyzer.execute(filterQuery)
      }
      (rewriteQuery, updatePartitions)
    } else {
      throw new AnalysisException(s"Table or view not found: ${source.get.identifier}")
    }
  }
}
