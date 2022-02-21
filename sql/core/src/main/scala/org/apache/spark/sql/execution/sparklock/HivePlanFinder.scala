package org.apache.spark.sql.execution.sparklock

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.ql.QueryPlan
import org.apache.hadoop.hive.ql.hooks.{Entity, ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.metadata.{Partition, Table}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.sparklock.SparkLockUtils.{getFieldVal, makeQueryId}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.internal.SessionState

import java.util
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object HivePlanFinder {
  lazy val hive: HiveMetaStoreClient = {
    val sparkConf = new SparkConf
    val hiveConf = new HiveConf(classOf[SessionState])
    sparkConf.getAll.toMap.foreach { case (k, v) =>
      hiveConf.set(k, v)
    }
    new HiveMetaStoreClient(hiveConf)
  }


  def buildHivePlan(plan: SparkPlan, context: SparkLockContext): QueryPlan = {
    //1. 找出output，库/表/分区
    val writeEntities = getOutputEntities(plan)
    // 2. 找出input，库/表/分区
    val readEntities = getInputEntities(plan, context.sparkSession)

    val hivePlan = new QueryPlan
    hivePlan.setQueryId(makeQueryId)
    hivePlan.setOutputs(writeEntities)
    hivePlan.setInputs(readEntities)

    hivePlan
  }

  def getInputEntities(plan: SparkPlan, sparkSession: SparkSession): util.HashSet[ReadEntity] = {
    def getTableMeta(identifier: TableIdentifier): (Database, Table) = {
      val dbName = identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
      val tableName = identifier.table
      getTableInt(dbName, tableName)
    }

    val readEntities = new util.HashSet[ReadEntity]

    plan transformDown {
      case dataSource: FileSourceScanExec =>
        if (dataSource.tableIdentifier.isDefined) {
          val identifier = dataSource.tableIdentifier.get
          val (metaDatabase, table) = getTableMeta(identifier)

          val partitions = new ArrayBuffer[String]()
          dataSource.selectedPartitions.foreach({ p =>
            val size = dataSource.relation.partitionSchema.size
            if (p.values.numFields == size) {
              val partName = new ListBuffer[String]()
              for (i <- 0 until size) {
                partName += s"${dataSource.relation.partitionSchema(i).name}=${p.values.getString(i)}"
              }
              partitions += partName.mkString("/")
            }
          })

          buildInputEntities(partitions, readEntities, metaDatabase, table)
        }

        dataSource
      case insertHive if insertHive.getClass.getSimpleName == "HiveTableScanExec" =>
        // HiveTableScanExec
        val identifier = getFieldVal(insertHive, "relation")
          .asInstanceOf[HiveTableRelation]
          .tableMeta.identifier
        val (metaDatabase, table) = getTableMeta(identifier)
        val partitions = getFieldVal(insertHive, "prunedPartitions").asInstanceOf[Seq[Partition]].map(p =>
          p.getName
        )
        buildInputEntities(partitions, readEntities, metaDatabase, table)

        insertHive
    }

    readEntities
  }

  def getOutputEntities(plan: SparkPlan): util.HashSet[WriteEntity] = {
    def getTableMeta(catalogTable: CatalogTable): (Database, Table) = {
      val dbName = catalogTable.database
      val tableName = catalogTable.identifier.table
      getTableInt(dbName, tableName)
    }

    val writeEntities = new util.HashSet[WriteEntity]()
    val writeEntity = new WriteEntity
    writeEntity.setWriteType(WriteEntity.WriteType.INSERT_OVERWRITE)
    plan match {
      case dwc: DataWritingCommandExec =>
        dwc.cmd match {
          case i@InsertIntoHadoopFsRelationCommand(_, staticPartitions, _, _, _, _, _, _, _, catalogTable, _, _) =>
            // InsertIntoHadoopFsRelationCommand
            if (catalogTable.isDefined) {
              val partName: Option[String] = {
                if (staticPartitions.nonEmpty && !i.dynamicPartitionOverwrite) {
                  Option(staticPartitions.map({ case (k, v) => s"$k=$v" }).mkString("/"))
                } else {
                  None
                }
              }
              val (metaDatabase, table) = getTableMeta(catalogTable.get)
              buildOutputEntities(writeEntity, metaDatabase, table, partName)
            }
          case insertHive if insertHive.nodeName == "InsertIntoHiveTable" =>
            // InsertIntoHiveTable
            val catalogTable = getFieldVal(insertHive, "table").asInstanceOf[CatalogTable]
            val _partition = getFieldVal(insertHive, "partition").asInstanceOf[Map[String, Option[String]]]
            val partName: Option[String] = {
              if (_partition.values.forall(_.isDefined)) {
                Option(_partition.map({ case (k, v) => s"$k=${v.get}" }).mkString("/"))
              } else {
                None
              }
            }
            val (metaDatabase, table) = getTableMeta(catalogTable)
            buildOutputEntities(writeEntity, metaDatabase, table, partName)
        }
    }
    writeEntities.add(writeEntity)

    writeEntities
  }

  private def buildInputEntities(partitions: Seq[String],
                         readEntities: util.HashSet[ReadEntity],
                         metaDatabase: Database,
                         table: Table): Unit = {
    if (partitions.isEmpty) {
      val readEntity = new ReadEntity()
      readEntity.setTyp(Entity.Type.TABLE)
      readEntity.setDatabase(metaDatabase)
      readEntity.setT(table)
      readEntities.add(readEntity)
    } else {
      partitions.foreach(p => {
        val readEntity = new ReadEntity
        readEntity.setTyp(Entity.Type.PARTITION)
        readEntity.setDatabase(metaDatabase)
        readEntity.setT(table)
        val partition = hive.getPartition(metaDatabase.getName, table.getTableName, p)
        readEntity.setP(new Partition(table, partition))
        readEntities.add(readEntity)
      })
    }
  }

  private def buildOutputEntities(writeEntity: WriteEntity, metaDatabase: Database, table: Table, partName: Option[String]): Unit = {
    writeEntity.setDatabase(metaDatabase)
    writeEntity.setT(table)
    if (partName.isDefined) {
      writeEntity.setTyp(Entity.Type.PARTITION)
      val partition = hive.getPartition(table.getDbName, table.getTableName, partName.get)
      writeEntity.setP(new Partition(table, partition))
    } else {
      writeEntity.setTyp(Entity.Type.TABLE)
    }
  }

  def getTableInt(dbName: String, tableName: String): (Database, Table) = {
    val metaDatabase = hive.getDatabase(dbName)
    val metaTable = hive.getTable(dbName, tableName)
    val table = new Table(metaTable)
    (metaDatabase, table)
  }
}
