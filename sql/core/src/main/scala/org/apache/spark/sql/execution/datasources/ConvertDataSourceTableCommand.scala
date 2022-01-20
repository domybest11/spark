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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{ConvertTableBaseCommand, DataWritingCommand}


case class ConvertDataSourceTableCommand(
    catalogTable: CatalogTable,
    query: LogicalPlan,
    fileFormat: Option[String],
    compressType: Option[String],
    updatePartitions: Seq[CatalogTablePartition] = Seq.empty,
    relation: Option[HadoopFsRelation])
  extends ConvertTableBaseCommand {

  override def getWritingCommand(session: SparkSession): DataWritingCommand = {
    if (!isHiveTable) {
      throw new AnalysisException("only support hive table.")
    }
    val hadoopFsRelation = relation.get
    val partitionSchema = query.resolve(
      hadoopFsRelation.partitionSchema, session.sessionState.analyzer.resolver)

    InsertIntoHadoopFsRelationCommand(
      hadoopFsRelation.location.rootPaths.head,
      Map.empty[String, String],
      ifPartitionNotExists = false,
      partitionSchema,
      hadoopFsRelation.bucketSpec,
      hadoopFsRelation.fileFormat,
      hadoopFsRelation.options,
      query,
      SaveMode.Overwrite,
      Some(catalogTable),
      Some(hadoopFsRelation.location),
      query.output.map(_.name))
  }
}
