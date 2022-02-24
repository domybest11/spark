package org.apache.spark.sql.execution.sparklock

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.ql.QueryPlan
import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.metadata.{DummyPartition, Table}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.sparklock.SparkLockUtils.{buildHiveConf, getFieldVal, makeQueryId}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

import java.util
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object HivePlanFinder {
  lazy val hive: HiveMetaStoreClient = {
    val sparkConf = SparkSession.active.sparkContext.conf
    new HiveMetaStoreClient(buildHiveConf(sparkConf))
  }


  def buildHivePlan(plan: SparkPlan, context: SparkLockContext): QueryPlan = {
    val writeEntities = new util.HashSet[WriteEntity]
    val readEntities = new util.HashSet[ReadEntity]

    //1. 找出output，库/表/分区
    parseOutputs(plan, writeEntities)

    // 2. 找出input，库/表/分区
    parseInputs(plan, readEntities, context.sparkSession)

    val hivePlan = new QueryPlan
    hivePlan.setQueryId(makeQueryId)
    hivePlan.setOutputs(writeEntities)
    hivePlan.setInputs(readEntities)

    hivePlan
  }

  def parseInputs(
                          plan: SparkPlan,
                          readEntities: util.HashSet[ReadEntity],
                          sparkSession: SparkSession
                        ): Unit = {
    def getTableMeta(identifier: TableIdentifier): Table = {
      val dbName = identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
      val tableName = identifier.table
      getTableInt(dbName, tableName)
    }

    plan transformDown {
      case dataSource: FileSourceScanExec =>
        if (dataSource.tableIdentifier.isDefined) {
          val identifier = dataSource.tableIdentifier.get
          val table = getTableMeta(identifier)

          val partitions = new ArrayBuffer[String]()
          dataSource.selectedPartitions.foreach({ p =>
            val size = dataSource.relation.partitionSchema.size
            if (size > 0 && p.values.numFields == size) {
              val partName = new ListBuffer[String]()
              for (i <- 0 until size) {
                partName += s"${dataSource.relation.partitionSchema(i).name}=${p.values.getString(i)}"
              }
              partitions += partName.mkString("/")
            }
          })

          buildInputEntities(partitions, readEntities, table)
        }
        dataSource

      case insertHive if insertHive.getClass.getSimpleName == "HiveTableScanExec" =>
        // HiveTableScanExec
        val relation = getFieldVal(insertHive, "relation")
          .asInstanceOf[HiveTableRelation]

        val table = getTableMeta(relation.tableMeta.identifier)
        val partitions = new ArrayBuffer[String]()
        relation.prunedPartitions.get.foreach(hd => {
          val size = relation.partitionCols.size
          val partSpec = hd.spec
          if (size > 0 && partSpec.size == size) {
            partitions += partSpec.map({ case (k, v) => s"$k=$v" }).mkString("/")
          }
        })
        buildInputEntities(partitions, readEntities, table)
        insertHive

      case adp: AdaptiveSparkPlanExec =>
        adp.inputPlan

      case other: SparkPlan => other
    }
  }

  def parseOutputs(
                           plan: SparkPlan,
                           writeEntities: util.HashSet[WriteEntity]
                         ): Unit = {
    def getTableMeta(catalogTable: CatalogTable): Table = {
      val dbName = catalogTable.database
      val tableName = catalogTable.identifier.table
      getTableInt(dbName, tableName)
    }

    plan match {
      case dwc: DataWritingCommandExec =>
        dwc.cmd match {
          case InsertIntoHadoopFsRelationCommand(_, staticPartitions, _, partitionColumns, _, _, _, _, _, catalogTable, _, _) =>
            // InsertIntoHadoopFsRelationCommand
            if (catalogTable.isDefined) {
              val (dynamicPartWrite, partName: Option[String]) = {
                if (staticPartitions.nonEmpty && staticPartitions.size == partitionColumns.length) {
                  (false, Option(staticPartitions.map({ case (k, v) => s"$k=$v" }).mkString("/")))
                } else {
                  (partitionColumns.nonEmpty, None)
                }
              }
              val table = getTableMeta(catalogTable.get)
              buildOutputEntities(writeEntities, table, partName, dynamicPartWrite)
            }
          case insertHive if insertHive.nodeName == "InsertIntoHiveTable" =>
            // InsertIntoHiveTable
            val catalogTable = getFieldVal(insertHive, "table").asInstanceOf[CatalogTable]
            val _partition = getFieldVal(insertHive, "partition").asInstanceOf[Map[String, Option[String]]]

            val partTable = catalogTable.partitionColumnNames.nonEmpty
            val (dynamicPartWrite, partName: Option[String]) = {
              if (partTable && _partition.values.forall(_.isDefined)) {
                (false, Option(_partition.map({ case (k, v) => s"$k=${v.get}" }).mkString("/")))
              } else {
                (partTable, None)
              }
            }
            val table = getTableMeta(catalogTable)
            buildOutputEntities(writeEntities, table, partName, dynamicPartWrite)

          case _ =>
        }
      case _: SparkPlan =>
    }
  }

  private def buildInputEntities(partitions: Seq[String],
                         readEntities: util.HashSet[ReadEntity],
                         table: Table): Unit = {
    val readEntity = new ReadEntity(table)
    readEntities.add(readEntity)
    if (partitions.nonEmpty) {
      partitions.foreach(p => {
        val partition = new DummyPartition(table, p)
        val readEntity = new ReadEntity(partition)
        readEntities.add(readEntity)
      })
    }
  }

  private def buildOutputEntities(
                                   writeEntities: util.HashSet[WriteEntity],
                                   table: Table,
                                   partName: Option[String],
                                   dynamicPartWrite: Boolean
                                 ): Unit = {
    if (partName.isDefined) {
      val partition = new DummyPartition(table, partName.get)
      val writeEntity = new WriteEntity(partition, WriteEntity.WriteType.INSERT_OVERWRITE)
      writeEntities.add(writeEntity)
    } else {
      val writeEntity = new WriteEntity(table, WriteEntity.WriteType.INSERT_OVERWRITE)
      writeEntity.setDynamicPartitionWrite(dynamicPartWrite)
      writeEntities.add(writeEntity)
    }
  }

  def getTableInt(dbName: String, tableName: String): Table = {
    val metaTable = hive.getTable(dbName, tableName)
    val table = new Table(metaTable)
    table
  }
}
