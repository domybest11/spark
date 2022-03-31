package org.apache.spark.sql.execution.sparklock

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.ql.QueryPlan
import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.metadata.{DummyPartition, Table}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, HiveTableRelation}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{ConvertTableBaseCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.sparklock.SparkLockUtils.{buildHiveConf, getFieldVal, makeQueryId}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, UnionExec}
import org.apache.spark.sql.internal.StaticSQLConf.{LOCK_MAX_HMS_CLIENTS_COUNT, LOCK_SPECIAL_USERS}

import java.util
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object HivePlanFinder extends Logging {
  val sparkConf: SparkConf = SparkSession.active.sparkContext.conf
  val specialHmsUsers: Seq[String] =
    sparkConf.get(LOCK_SPECIAL_USERS).getOrElse(Seq.empty)
  lazy val maxHmsClientCount: Int =
    sparkConf.get(LOCK_MAX_HMS_CLIENTS_COUNT)
  lazy val hiveConf: HiveConf = {
    buildHiveConf(sparkConf)
  }

  private val hiveClientList =
    new ConcurrentHashMap[String, CopyOnWriteArrayList[HiveMetaStoreClient]]

  def hive: HiveMetaStoreClient = {
    val user = UserGroupInformation.getCurrentUser.getShortUserName
    val clients = hiveClientList.compute(user, (_, oldClients) => {
      if (oldClients == null) {
        val newClients = new CopyOnWriteArrayList[HiveMetaStoreClient]
        if (specialHmsUsers.contains(user)) {
          logInfo(s"create $maxHmsClientCount hms clients for $user")
          (0 until maxHmsClientCount).foreach(
            _ => newClients.add(new HiveMetaStoreClient(hiveConf))
          )
        } else {
          logInfo(s"create one hms client for $user")
          newClients.add(new HiveMetaStoreClient(hiveConf))
        }
        newClients
      } else {
        oldClients
      }
    })
    if (clients.size() == 1) {
      clients.get(0)
    } else {
      clients.get(Random.nextInt(maxHmsClientCount))
    }
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
          val partitionSchema = dataSource.relation.partitionSchema
          val partColSize = partitionSchema.size
          if (partColSize > 0 && dataSource.selectedPartitions.nonEmpty) {
            dataSource.selectedPartitions.foreach({ p =>
              if (p.values.numFields == partColSize) {
                val partName = new ListBuffer[String]()
                for (i <- 0 until partColSize) {
                  partName += s"${partitionSchema(i).name}=${p.values.get(i, partitionSchema(i).dataType)}"
                }
                partitions += partName.mkString("/")
              }
            })
          }

          buildInputEntities(partitions, readEntities, table)
        }
        dataSource

      case hiveTableScan if hiveTableScan.getClass.getSimpleName == "HiveTableScanExec" =>
        // HiveTableScanExec
        val relation = getFieldVal(hiveTableScan, "relation")
          .asInstanceOf[HiveTableRelation]

        val table = getTableMeta(relation.tableMeta.identifier)
        val partitions = new ArrayBuffer[String]()
        val partColSize = relation.partitionCols.size
        if (partColSize > 0
          && relation.prunedPartitions.isDefined
          && relation.prunedPartitions.get.nonEmpty
        ) {
          relation.prunedPartitions.get.foreach(hd =>
            buildPartName(hd, partitions, partColSize)
          )
        }
        buildInputEntities(partitions, readEntities, table)
        hiveTableScan

      case adp: AdaptiveSparkPlanExec =>
        adp.inputPlan

      case other: SparkPlan => other
    }
  }

  def parseOutputs(
                    plan: SparkPlan,
                    writeEntities: util.HashSet[WriteEntity]
                  ): Unit = {
    plan match {
      case UnionExec(children) =>
        children.foreach {
          case dwc: DataWritingCommandExec =>
            parseDataWritingCommand(dwc, writeEntities)
          case _: SparkPlan =>
        }

      case dwc: DataWritingCommandExec =>
        parseDataWritingCommand(dwc, writeEntities)

      case _: SparkPlan =>
    }
  }

  private def parseDataWritingCommand(
                                       dwc: DataWritingCommandExec,
                                       writeEntities: util.HashSet[WriteEntity]
                                     ): Unit = {
    def getTableMeta(catalogTable: CatalogTable): Table = {
      val dbName = catalogTable.database
      val tableName = catalogTable.identifier.table
      getTableInt(dbName, tableName)
    }

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

      case convert: ConvertTableBaseCommand =>
        val catalogTable = convert.catalogTable
        val table = getTableMeta(catalogTable)

        val partitions = new ArrayBuffer[String]()
        val partColSize = catalogTable.partitionColumnNames.size
        if (partColSize > 0 && convert.updatePartitions.nonEmpty) {
          convert.updatePartitions.foreach({ p =>
            buildPartName(p, partitions, partColSize)
          })
        }
        if (partitions.isEmpty) {
          buildOutputEntities(writeEntities, table, None, dynamicPartWrite = false)
        } else {
          partitions.foreach(part => buildOutputEntities(writeEntities, table, Option(part), dynamicPartWrite = false))
        }

      case _ =>
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

  private def buildPartName(p: CatalogTablePartition, partitions: ArrayBuffer[String], partColSize: Int): Unit = {
    val spec = p.spec
    if (spec.size == partColSize) {
      partitions += spec.map({ case (k, v) => s"$k=$v" }).mkString("/")
    }
  }
}
