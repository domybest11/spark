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

import java.util.Random

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, RebalancePartitions, Repartition, RepartitionByExpression, Sort, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand}
import org.apache.spark.sql.execution.datasources.{ConvertDataSourceTableCommand, InsertIntoHadoopFsRelationCommand, MergeDataSourceTableCommand}
import org.apache.spark.sql.hive.RepartitionBeforeWriteHelper.{canInsertRebalancePartitions, createExtraExpression}
import org.apache.spark.sql.hive.execution.{ConvertHiveTableCommand, CreateHiveTableAsSelectCommand, InsertIntoHiveDirCommand, InsertIntoHiveTable, MergeHiveTableCommand, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * For Hive/datasource table, there five commands can write data to table
 * 1. InsertIntoHiveTable
 * 2. CreateHiveTableAsSelectCommand
 * 3. OptimizedCreateHiveTableAsSelectCommand
 * 4. InsertIntoHadoopFsRelationCommand
 * 5. CreateDataSourceTableAsSelectCommand
 * This rule add a repartition node between write and query
*/
case class RebalancePartitionBeforeWriteTable(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive" &&
      conf.getConf(SQLConf.INSERT_REBALANCEPARTITIONS_BEFORE_WRITE)) {
      addRebalancePartition(plan)
    } else {
      plan
    }
  }

  def applyRebalancePartition(
      dataWriting: DataWritingCommand,
      bucketSpec: Option[BucketSpec],
      applyColumns: Seq[Expression]): LogicalPlan = {
    val query = dataWriting.query
    (bucketSpec, applyColumns) match {
      case (None, partExps) =>
        if (partExps.nonEmpty) {
          dataWriting.withNewChildren(IndexedSeq(RebalancePartitions(partExps, query)))
        } else {
          dataWriting.withNewChildren(IndexedSeq(RebalancePartitions(Seq.empty, query))
          )
        }
      case (Some(bucket), partExps) =>
        if (dataWriting.isInstanceOf[InsertIntoHiveTable] ||
          dataWriting.isInstanceOf[InsertIntoHiveDirCommand] ||
          dataWriting.isInstanceOf[CreateHiveTableAsSelectCommand] ||
          dataWriting.isInstanceOf[OptimizedCreateHiveTableAsSelectCommand]
        ) {
          dataWriting
        } else {
          if (bucket.sortColumnNames.nonEmpty) {
            val sortExps = resolveColumnNames(bucket.sortColumnNames, query.output)
              .map(SortOrder(_, Ascending))
            dataWriting.withNewChildren(
              IndexedSeq(Sort(sortExps, false, RebalancePartitions(partExps, query)))
            )
          } else {
            dataWriting.withNewChildren(
              IndexedSeq(RebalancePartitions(partExps, query))
            )
          }
        }
      case _ => dataWriting
    }
  }

  private def resolveColumnNames(
      columnNames: Seq[String],
      outputAttrs: Seq[Attribute]): Seq[NamedExpression] = {
    columnNames.map { c =>
      outputAttrs.resolve(c :: Nil, conf.resolver).
        getOrElse(throw new SparkException(s"Cannot resolve column name $c among (" +
          s"${outputAttrs.map(_.name).mkString(",")})."))
    }
  }

  private def getRebalanceColumns(
      query: LogicalPlan,
      table: CatalogTable): Seq[Expression] = {
    val dynamicPartitionColumns = query.output.filter(attr =>
      table.partitionColumnNames.contains(attr.name))
    if (dynamicPartitionColumns.isEmpty) {
      dynamicPartitionColumns ++ createExtraExpression
    } else {
      dynamicPartitionColumns
    }
  }

  def addRebalancePartition(plan: LogicalPlan): LogicalPlan = plan match {
    case u: Union =>
      u.withNewChildren(u.children.map(addRebalancePartition))

    case i @ InsertIntoHiveTable(table, _, query, _, _, _)
        if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(i, table.bucketSpec, getRebalanceColumns(query, table))

    case c @ CreateHiveTableAsSelectCommand(table, query, _, _)
        if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(c, table.bucketSpec, getRebalanceColumns(query, table))

    case o @ OptimizedCreateHiveTableAsSelectCommand(table, query, _, _)
        if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(o, table.bucketSpec, getRebalanceColumns(query, table))

    case i @ InsertIntoHadoopFsRelationCommand(_, _, _, _, _, _, _, query, _, table, _, _)
        if query.resolved && table.isDefined && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(i, table.get.bucketSpec, getRebalanceColumns(query, table.get))

    case o @ CreateDataSourceTableAsSelectCommand(table, _, query, _)
        if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(o, table.bucketSpec, getRebalanceColumns(query, table))

    case o @ MergeHiveTableCommand(table, query, _)
      if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(o, table.bucketSpec, getRebalanceColumns(query, table))

    case o @ MergeDataSourceTableCommand(table, query, _)
      if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(o, table.bucketSpec, getRebalanceColumns(query, table))

    case o @ ConvertHiveTableCommand(table, query, _, _, _, _)
      if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(o, table.bucketSpec, getRebalanceColumns(query, table))

    case o @ ConvertDataSourceTableCommand(table, query, _, _, _, _)
      if query.resolved && canInsertRebalancePartitions(query) =>
      applyRebalancePartition(o, table.bucketSpec, getRebalanceColumns(query, table))

    case _ => plan
  }
}

object RepartitionBeforeWriteHelper {
  def canInsertRebalancePartitions(plan: LogicalPlan): Boolean = plan match {
    case Limit(_, _) => false
    case ScanOperation(_, _, child) =>
      child match {
        case _: RepartitionByExpression => false
        case _: Repartition => false
        case _: RebalancePartitions => false
        case _: Sort => false
        case _ => true
      }
    case _ => true
  }

  def createExtraExpression(): Seq[Expression] = {
    Literal(new Random().nextLong()) :: Nil
  }

}
