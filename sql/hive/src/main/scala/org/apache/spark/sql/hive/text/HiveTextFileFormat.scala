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

package org.apache.spark.sql.hive.text

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructType, UserDefinedType}
import org.apache.spark.util.SerializableConfiguration

/**
 * A data source file format for reading hive text table.
 */
class HiveTextFileFormat extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "hive_text"

  override def toString: String = "Hive_Text"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException(s"inferSchema is not supported for $this.")
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(s"prepareWrite is not supported for $this.")
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      tableOpt: Option[CatalogTable],
      partitionOpt: Option[CatalogTablePartition]): (PartitionedFile) => Iterator[InternalRow] = {
    val broadcastHadoopConf = {
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    }
    val lineDelim = options.get("line.delim").map(_.getBytes)
    if (tableOpt.isDefined && partitionOpt.isDefined) {
      val hiveTable = HiveClientImpl.toHiveTable(tableOpt.get)
      val tableDeser = new TableDesc(
        hiveTable.getInputFormatClass,
        hiveTable.getOutputFormatClass,
        hiveTable.getMetadata)
      val hivePartition = HiveClientImpl.toHivePartition(partitionOpt.get, hiveTable)
      val partDeser = hivePartition.getDeserializer.getClass.asInstanceOf[Class[Deserializer]]
      val tableProperties = tableDeser.getProperties
      val partProps = hivePartition.getMetadataFromPartitionSchema
      val localTableDesc = tableDeser
      val localPartDesc = partDeser

      (file: PartitionedFile) => {
        val hConf = broadcastHadoopConf.value.value
        val localPartSerDe = localPartDesc.getConstructor().newInstance()
        val props = new Properties(tableProperties)

        partProps.forEach {
          case (key, value) => props.setProperty(key.toString, value.toString)
        }
        DeserializerLock.synchronized {
          localPartSerDe.initialize(hConf, props)
        }
        val localTableSerDe = localTableDesc.getDeserializerClass.getConstructor().newInstance()
        DeserializerLock.synchronized {
          localTableSerDe.initialize(hConf, tableProperties)
        }
        val mutableRow = new SpecificInternalRow(requiredSchema.map(_.dataType))
        val textDeserializer =
          new HiveTextDeserializer(localTableSerDe, localPartSerDe, mutableRow, requiredSchema)
        val iterator = new HadoopFileLinesReader(file, lineDelim, broadcastHadoopConf.value.value)
        val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
        val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

        if (partitionSchema.length == 0) {
          iterator.map(line => unsafeProjection(textDeserializer.deserialize(line)))
        } else {
          val joinedRow = new JoinedRow()
          iterator.map(line =>
            unsafeProjection(joinedRow(textDeserializer.deserialize(line), file.partitionValues)))
        }
      }
    } else {
      throw new UnsupportedOperationException("buildReaderWithPartitionValues Error: " +
        "table or partition are None")
    }
  }

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }

}

object DeserializerLock

