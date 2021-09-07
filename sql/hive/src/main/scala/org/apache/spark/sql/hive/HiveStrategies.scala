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

import java.io.IOException
import java.util.{Locale, Random}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoDir, InsertIntoStatement, Limit, LogicalPlan, Repartition, RepartitionByExpression, ScriptTransformation, Sort, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Util.assertNoNullTypeInSchema
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSourceStrategy, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.hive.RepartitionBeforeWriteHelper._
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.hive.execution.HiveScriptTransformationExec
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.IntegerType


/**
 * Determine the database, serde/format and schema of the Hive serde table, according to the storage
 * properties.
 */
class ResolveHiveSerdeTable(session: SparkSession) extends Rule[LogicalPlan] {
  private def determineHiveSerde(table: CatalogTable): CatalogTable = {
    if (table.storage.serde.nonEmpty) {
      table
    } else {
      if (table.bucketSpec.isDefined) {
        throw new AnalysisException("Creating bucketed Hive serde table is not supported yet.")
      }

      val defaultStorage = HiveSerDe.getDefaultStorage(conf)
      val options = new HiveOptions(table.storage.properties)

      val fileStorage = if (options.fileFormat.isDefined) {
        HiveSerDe.sourceToSerDe(options.fileFormat.get) match {
          case Some(s) =>
            CatalogStorageFormat.empty.copy(
              inputFormat = s.inputFormat,
              outputFormat = s.outputFormat,
              serde = s.serde)
          case None =>
            throw new IllegalArgumentException(s"invalid fileFormat: '${options.fileFormat.get}'")
        }
      } else if (options.hasInputOutputFormat) {
        CatalogStorageFormat.empty.copy(
          inputFormat = options.inputFormat,
          outputFormat = options.outputFormat)
      } else {
        CatalogStorageFormat.empty
      }

      val rowStorage = if (options.serde.isDefined) {
        CatalogStorageFormat.empty.copy(serde = options.serde)
      } else {
        CatalogStorageFormat.empty
      }

      val storage = table.storage.copy(
        inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
        outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
        serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
        properties = options.serdeProperties)

      table.copy(storage = storage)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c @ CreateTable(t, _, query) if DDLUtils.isHiveTable(t) =>
      // Finds the database name if the name does not exist.
      val dbName = t.identifier.database.getOrElse(session.catalog.currentDatabase)
      val table = t.copy(identifier = t.identifier.copy(database = Some(dbName)))

      // Determines the serde/format of Hive tables
      val withStorage = determineHiveSerde(table)

      // Infers the schema, if empty, because the schema could be determined by Hive
      // serde.
      val withSchema = if (query.isEmpty) {
        val inferred = HiveUtils.inferSchema(withStorage)
        if (inferred.schema.length <= 0) {
          throw new AnalysisException("Unable to infer the schema. " +
            s"The schema specification is required to create the table ${inferred.identifier}.")
        }
        inferred
      } else {
        withStorage
      }

      c.copy(tableDesc = withSchema)
  }
}

class DetermineTableStats(session: SparkSession) extends Rule[LogicalPlan] {
  private def hiveTableWithStats(relation: HiveTableRelation): HiveTableRelation = {
    val table = relation.tableMeta
    val partitionCols = relation.partitionCols
    // For partitioned tables, the partition directory may be outside of the table directory.
    // Which is expensive to get table size. Please see how we implemented it in the AnalyzeTable.
    val sizeInBytes = if (conf.fallBackToHdfsForStatsEnabled && partitionCols.isEmpty) {
      try {
        val hadoopConf = session.sessionState.newHadoopConf()
        val tablePath = new Path(table.location)
        val fs: FileSystem = tablePath.getFileSystem(hadoopConf)
        fs.getContentSummary(tablePath).getLength
      } catch {
        case e: IOException =>
          logWarning("Failed to get table size from HDFS.", e)
          conf.defaultSizeInBytes
      }
    } else {
      conf.defaultSizeInBytes
    }

    val stats = Some(Statistics(sizeInBytes = BigInt(sizeInBytes)))
    relation.copy(tableStats = stats)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty =>
      hiveTableWithStats(relation)

    // handles InsertIntoStatement specially as the table in InsertIntoStatement is not added in its
    // children, hence not matched directly by previous HiveTableRelation case.
    case i @ InsertIntoStatement(relation: HiveTableRelation, _, _, _, _, _)
      if DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty =>
      i.copy(table = hiveTableWithStats(relation))
  }
}

/**
 * Replaces generic operations with specific variants that are designed to work with Hive.
 *
 * Note that, this rule must be run after `PreprocessTableCreation` and
 * `PreprocessTableInsertion`.
 */
object HiveAnalysis extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case InsertIntoStatement(
        r: HiveTableRelation, partSpec, _, query, overwrite, ifPartitionNotExists)
        if DDLUtils.isHiveTable(r.tableMeta) =>
      InsertIntoHiveTable(r.tableMeta, partSpec, query, overwrite,
        ifPartitionNotExists, query.output.map(_.name))

    case CreateTable(tableDesc, mode, None) if DDLUtils.isHiveTable(tableDesc) =>
      CreateTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query))
        if DDLUtils.isHiveTable(tableDesc) && query.resolved =>
      CreateHiveTableAsSelectCommand(tableDesc, query, query.output.map(_.name), mode)

    case InsertIntoDir(isLocal, storage, provider, child, overwrite)
        if DDLUtils.isHiveTable(provider) && child.resolved =>
      val outputPath = new Path(storage.locationUri.get)
      if (overwrite) DDLUtils.verifyNotReadPath(child, outputPath)

      InsertIntoHiveDirCommand(isLocal, storage, child, overwrite, child.output.map(_.name))
  }
}

/**
 * Relation conversion from metastore relations to data source relations for better performance
 *
 * - When writing to non-partitioned Hive-serde Parquet/Orc tables
 * - When scanning Hive-serde Parquet/ORC tables
 *
 * This rule must be run before all other DDL post-hoc resolution rules, i.e.
 * `PreprocessTableCreation`, `PreprocessTableInsertion`, `DataSourceAnalysis` and `HiveAnalysis`.
 */
case class RelationConversions(
    sessionCatalog: HiveSessionCatalog) extends Rule[LogicalPlan] {
  private def isConvertible(relation: HiveTableRelation): Boolean = {
    isConvertible(relation.tableMeta)
  }

  private def isConvertible(tableMeta: CatalogTable): Boolean = {
    val serde = tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    serde.contains("parquet") && SQLConf.get.getConf(HiveUtils.CONVERT_METASTORE_PARQUET) ||
      serde.contains("orc") && SQLConf.get.getConf(HiveUtils.CONVERT_METASTORE_ORC)
  }

  private val metastoreCatalog = sessionCatalog.metastoreCatalog

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      // Write path
      case InsertIntoStatement(
          r: HiveTableRelation, partition, cols, query, overwrite, ifPartitionNotExists)
          if query.resolved && DDLUtils.isHiveTable(r.tableMeta) &&
            (!r.isPartitioned || SQLConf.get.getConf(HiveUtils.CONVERT_INSERTING_PARTITIONED_TABLE))
            && isConvertible(r) =>
        InsertIntoStatement(metastoreCatalog.convert(r), partition, cols,
          query, overwrite, ifPartitionNotExists)

      // Read path
      case relation: HiveTableRelation
          if DDLUtils.isHiveTable(relation.tableMeta) && isConvertible(relation) =>
        metastoreCatalog.convert(relation)

      // CTAS
      case CreateTable(tableDesc, mode, Some(query))
          if query.resolved && DDLUtils.isHiveTable(tableDesc) &&
            tableDesc.partitionColumnNames.isEmpty && isConvertible(tableDesc) &&
            SQLConf.get.getConf(HiveUtils.CONVERT_METASTORE_CTAS) =>
        // validation is required to be done here before relation conversion.
        DDLUtils.checkDataColNames(tableDesc.copy(schema = query.schema))
        // This is for CREATE TABLE .. STORED AS PARQUET/ORC AS SELECT null
        assertNoNullTypeInSchema(query.schema)
        OptimizedCreateHiveTableAsSelectCommand(
          tableDesc, query, query.output.map(_.name), mode)
    }
  }
}

/**
 * For Hive/datasource table, there five commands can write data to table
 * 1. InsertIntoHiveTable
 * 2. CreateHiveTableAsSelectCommand
 * 3. OptimizedCreateHiveTableAsSelectCommand
 * 4. InsertIntoHadoopFsRelationCommand
 * 5. CreateDataSourceTableAsSelectCommand
 * This rule add a repartition node between write and query
 */
case class RepartitionBeforeWriteHive(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive" &&
      conf.getConf(SQLConf.INSERT_REPARTITION_BEFORE_WRITE)) {
      addRepartition(plan)
    } else {
      plan
    }
  }

  def addRepartition(plan: LogicalPlan): LogicalPlan = plan match {
    case i @ InsertIntoHiveTable(table, partition, query, _, _, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns = partition.filter(_._2.isEmpty).keys
        .flatMap(name => query.output.find(_.name == name)).toSeq
      if (dynamicPartitionColumns.isEmpty) {
        i.copy(query =
          RepartitionByExpression(
            Seq.empty,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      } else {
        val extended = dynamicPartitionColumns ++ dynamicPartitionExtraExpression(
          conf.getConf(SQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM))
        i.copy(query =
          RepartitionByExpression(
            extended,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      }

    case c @ CreateHiveTableAsSelectCommand(table, query, _, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns =
        query.output.filter(attr => table.partitionColumnNames.contains(attr.name))
      if (dynamicPartitionColumns.isEmpty) {
        c.copy(query =
          RepartitionByExpression(
            Seq.empty,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      } else {
        val extended = dynamicPartitionColumns ++ dynamicPartitionExtraExpression(
          conf.getConf(SQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM))
        c.copy(query =
          RepartitionByExpression(
            extended,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      }

    case o @ OptimizedCreateHiveTableAsSelectCommand(table, query, _, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns =
        query.output.filter(attr => table.partitionColumnNames.contains(attr.name))
      if (dynamicPartitionColumns.isEmpty) {
        o.copy(query =
          RepartitionByExpression(
            Seq.empty,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      } else {
        val extended = dynamicPartitionColumns ++ dynamicPartitionExtraExpression(
          conf.getConf(SQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM))
        o.copy(query =
          RepartitionByExpression(
            extended,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      }

    case i @ InsertIntoHadoopFsRelationCommand(_, _, _, _, _, _, _, query, _, table, _, _) =>
      if (query.resolved &&
        !table.getOrElse("").equals("") &&
        table.get.bucketSpec.isEmpty &&
        canInsertRepartitionByExpression(query)) {
        val dynamicPartitionColumns =
          query.output.filter(attr => table.get.partitionColumnNames.contains(attr.name))
        if (dynamicPartitionColumns.isEmpty) {
          i.copy(query =
            RepartitionByExpression(
              Seq.empty,
              query,
              conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
        } else {
          val extended = dynamicPartitionColumns ++ dynamicPartitionExtraExpression(
            conf.getConf(SQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM))
          i.copy(query =
            RepartitionByExpression(
              extended,
              query,
              conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
        }
      } else {
        i
      }

    case o @ CreateDataSourceTableAsSelectCommand(table, _, query, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns =
        query.output.filter(attr => table.partitionColumnNames.contains(attr.name))
      if (dynamicPartitionColumns.isEmpty) {
        o.copy(query =
          RepartitionByExpression(
            Seq.empty,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      } else {
        val extended = dynamicPartitionColumns ++ dynamicPartitionExtraExpression(
          conf.getConf(SQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM))
        o.copy(query =
          RepartitionByExpression(
            extended,
            query,
            conf.getConf(SQLConf.INSERT_REPARTITION_NUM)))
      }

    case _ => plan
  }
}

object RepartitionBeforeWriteHelper {
  def canInsertRepartitionByExpression(plan: LogicalPlan): Boolean = plan match {
    case Limit(_, _) => false
    case _: Sort => false
    case _: RepartitionByExpression => false
    case _: Repartition => false
    case _ => true
  }

  def dynamicPartitionExtraExpression(partitionNumber: Int): Seq[Expression] = {
    // Dynamic partition insertion will add repartition by partition column, but it could cause
    // data skew (one partition value has large data). So we add extra partition column for the
    // same dynamic partition to avoid skew.
    Cast(Multiply(
      new Rand(Literal(new Random().nextLong())),
      Literal(partitionNumber.toDouble)
    ), IntegerType) :: Nil
  }
}

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object HiveScripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = ScriptTransformationIOSchema(ioschema)
        HiveScriptTransformationExec(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ScanOperation(projectList, filters, relation: HiveTableRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionCols)
        val normalizedFilters = DataSourceStrategy.normalizeExprs(
          filters.filter(_.deterministic), relation.output)

        val partitionKeyFilters = DataSourceStrategy.getPushedDownFilters(relation.partitionCols,
          normalizedFilters)

        pruneFilterProject(
          projectList,
          filters.filter(f => f.references.isEmpty || !f.references.subsetOf(partitionKeyIds)),
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, partitionKeyFilters.toSeq)(sparkSession)) :: Nil
      case _ =>
        Nil
    }
  }
}
