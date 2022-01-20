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

package org.apache.spark.sql.hive.extensions

import java.util.Locale

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Ascending, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.hive.extensions.zorder.Zorder
import org.apache.spark.sql.internal.SQLConf.ZorderBuildStrategy

trait OptimizeLayoutBeforeWriting extends Rule[LogicalPlan] {
  private val SPARK_LAYOUT_OPTIMIZE_ENABLED = "spark.layout.optimize.enabled"
  private val SPARK_SORT_COLS = "spark.sort.cols"

  def optimizeLayoutEnabled(props: Map[String, String]): Boolean = {
    props.contains(SPARK_LAYOUT_OPTIMIZE_ENABLED) &&
      "true".equalsIgnoreCase(props(SPARK_LAYOUT_OPTIMIZE_ENABLED)) &&
      props.contains(SPARK_SORT_COLS)
  }

  def getSortCols(props: Map[String, String]): Seq[String] = {
    val cols = props.get(SPARK_SORT_COLS)
    assert(cols.isDefined)
    cols.get.split(",").map(_.trim.toLowerCase(Locale.ROOT))
  }

  def canInsertRepartitionByExpression(plan: LogicalPlan): Boolean = plan match {
    case Project(_, child) => canInsertRepartitionByExpression(child)
    case _: Sort => false
    case _: RepartitionByExpression => false
    case _: Repartition => false
    case _ => true
  }

  def redistribute(catalogTable: CatalogTable, plan: LogicalPlan): LogicalPlan = {
    if (!canInsertRepartitionByExpression(plan)) {
      return plan
    }
    val cols = getSortCols(catalogTable.properties)
    if (cols.length > 4) {
      return plan
    }
    val attrs = plan.output.map(attr => (attr.name, attr)).toMap
    if (cols.exists(!attrs.contains(_))) {
      logWarning(s"target table does not contain all sort cols: ${cols.mkString(",")}, " +
        s"please check your table properties ${SPARK_SORT_COLS}.")
      plan
    } else {
      val orderExpr = cols.map(attrs(_))
      if (orderExpr.length == 1) {
        Sort(SortOrder(orderExpr.head, Ascending, NullsLast, Seq.empty) :: Nil, true, plan)
      } else {
        conf.layoutOptimizeZorderStrategy match {
          case ZorderBuildStrategy.SAMPLE =>
            Sort(
              SortOrder(orderExpr.head, Ascending, NullsLast, Seq.empty) :: Nil,
              false,
              RepartitionByZorder(orderExpr, plan, Some(conf.numShufflePartitions))
            )
          case ZorderBuildStrategy.DIRECT =>
            Sort(SortOrder(Zorder(orderExpr), Ascending, NullsLast, Seq.empty) :: Nil, true, plan)
        }
      }
    }
  }

  def applyInternal(plan: LogicalPlan): LogicalPlan

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.layoutOptimizeEnabled) {
      applyInternal(plan)
    } else {
      plan
    }
  }
}

case class OptimizeLayoutBeforeWritingDatasource() extends OptimizeLayoutBeforeWriting {
  override def applyInternal(plan: LogicalPlan): LogicalPlan = plan match {
    case insert @ InsertIntoHadoopFsRelationCommand(_, _, _, _, bucket, _, _, query, _, table, _, _)
      if query.resolved && bucket.isEmpty && table.isDefined &&
        optimizeLayoutEnabled(table.get.properties) =>
      val newQuery = redistribute(table.get, query)
      if (newQuery.eq(query)) {
        insert
      } else {
        insert.copy(query = newQuery)
      }
    case _ => plan
  }
}

case class OptimizeLayoutBeforeWritingHive() extends OptimizeLayoutBeforeWriting {
  override def applyInternal(plan: LogicalPlan): LogicalPlan = {
    case insert: InsertIntoHiveTable
      if insert.query.resolved && insert.table.bucketSpec.isEmpty &&
        optimizeLayoutEnabled(insert.table.properties) =>
      val newQuery = redistribute(insert.table, insert.query)
      if (newQuery.eq(insert.query)) {
        insert
      } else {
        insert.copy(query = newQuery)
      }
    case _ => plan
  }
}
