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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, MergeTableStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf


case class MergePruning(session: SparkSession) extends Rule[LogicalPlan] {
  val avgConditionSize: Long = session.conf.get(SQLConf.MERGE_SMALLFILE_SIZE)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case MergeTableStatement(tableRelation @ LogicalRelation(
      t: HadoopFsRelation, _, _, _), query, _) =>
        val (pruning, staticPartitions) = rewriteQuery(query, t)
        MergeTableStatement(tableRelation, pruning, staticPartitions)
    }
  }

  private def rewriteQuery(query: LogicalPlan, relation: HadoopFsRelation):
  (LogicalPlan, Map[String, String]) = {
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
      val map = scala.collection.mutable.Map[String, String]()
      val rewriteQuery = if (source.get.partitionColumnNames.isEmpty) {
        val tableProperties = source.get.properties
        val totalSize = tableProperties.getOrElse("totalSize", "-1").toLong
        val numFiles = tableProperties.getOrElse("numFiles", "-1").toLong
        val needMerge = if (totalSize == -1L || numFiles == -1L) {
          true
        } else if (numFiles == 0) {
          false
        } else {
          totalSize / numFiles < avgConditionSize
        }
        if (needMerge) {
          query
        } else {
          throw new AnalysisException("table don't need merge.")
        }
      } else {
        val prunedPartition = catalog.listPartitionsByFilter(
          source.get.identifier, filter.map(f => f.condition).toSeq
        ).filter {
          partition =>
            val totalSize = partition.parameters
              .getOrElse("totalSize", "-1").toLong
            val numFiles = partition.parameters
              .getOrElse("numFiles", "-1").toLong

            if (totalSize == -1L || numFiles == -1L) {
              true
            } else if (numFiles == 0) {
              false
            } else {
              totalSize / numFiles < avgConditionSize
            }
        }

        val firstPartitionField = relation.partitionSchema.fields(0)
        val filterExpression = prunedPartition.map{
          partition =>
            val value = partition.spec(firstPartitionField.name)
            map += value ->firstPartitionField.name
            partition.spec.map(entry =>
              EqualTo(UnresolvedAttribute(Seq(entry._1)), Literal(entry._2))
            ).reduce((e1: Expression, e2: Expression) => And(e1, e2))
        }.toArray
        val filterQuery = if (filterExpression.isEmpty) {
          throw new AnalysisException("table don't need merge.")
        } else {
          val condition = filterExpression
            .reduce((pf1: Expression, pf2: Expression) => Or(pf1, pf2))
          Filter(condition, UnresolvedRelation(source.get.identifier))
        }
        session.sessionState.analyzer.execute(filterQuery)
      }
      if (map.size == 1) {
        val (value, key) = map.head
        (rewriteQuery, Map[String, String](key -> value))
      } else {
        (rewriteQuery, Map.empty)
      }
    } else {
      throw new AnalysisException(s"Table or view not found: ${source.get.identifier}")
    }
  }
}
