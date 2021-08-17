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

import java.io.IOException

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}

import org.apache.spark.SparkException
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.rdd.{RDD, UnionPartition, UnionRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.merge.MergeUtils
import org.apache.spark.sql.util.SchemaUtils



/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.
 *
 * @param staticPartitions partial partitioning spec for write. This defines the scope of partition
 *                         overwrites: when the spec is empty, all partitions are overwritten.
 *                         When it covers a prefix of the partition keys, only partitions matching
 *                         the prefix are overwritten.
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoHadoopFsRelationCommand(
    outputPath: Path,
    staticPartitions: TablePartitionSpec,
    ifPartitionNotExists: Boolean,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String],
    query: LogicalPlan,
    mode: SaveMode,
    catalogTable: Option[CatalogTable],
    fileIndex: Option[FileIndex],
    outputColumnNames: Seq[String])
  extends DataWritingCommand {

  private lazy val parameters = CaseInsensitiveMap(options)

  private[sql] lazy val dynamicPartitionOverwrite: Boolean = {
    val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
      // scalastyle:off caselocale
      .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase))
      // scalastyle:on caselocale
      .getOrElse(SQLConf.get.partitionOverwriteMode)
    val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
    // This config only makes sense when we are overwriting a partitioned dataset with dynamic
    // partition columns.
    enableDynamicOverwrite && mode == SaveMode.Overwrite &&
      (staticPartitions.size < partitionColumns.length || staticPartitions.nonEmpty)
  }


  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val mergeEnabled = sparkSession.sessionState.conf.mergeHiveFiles
    val avgConditionSize: Long = sparkSession.conf.get(SQLConf.MERGE_SMALLFILE_SIZE)
    val outputAverageSize: Long = sparkSession.conf.get(SQLConf.MERGE_FILE_PER_TASK)

    // Most formats don't do well with duplicate columns, so lets not allow that
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into $outputPath",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    var qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val finalOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog

    var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
    var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
    var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

    // When partitions are tracked by the catalog, compute all custom partition locations that
    // may be relevant to the insertion job.
    if (partitionsTrackedByCatalog) {
      matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
        catalogTable.get.identifier, Some(staticPartitions))
      initialMatchingPartitions = matchingPartitions.map(_.spec)
      customPartitionLocations = getCustomPartitionLocations(
        fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
    }
    if (SQLConf.get.mirrorExecute &&
      catalogTable.isDefined &&
      !catalogTable.get.identifier.database.get.startsWith("dolphin")
    ) {
      throw new SparkException(s"forbid insert into ${catalogTable.get.identifier.database.get}")
    }
    val canMerge = mergeEnabled && bucketSpec.isEmpty && customPartitionLocations.isEmpty

    val mergeDir = ".merge-temp-" + java.util.UUID.randomUUID().toString
    if (canMerge) {
      qualifiedOutputPath = new Path(qualifiedOutputPath, mergeDir)
      fs.deleteOnExit(qualifiedOutputPath)
    }
    logInfo("merge job tmp output path " + qualifiedOutputPath)

    val jobId = java.util.UUID.randomUUID().toString
    // if can merge writer data into tmp path
    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = jobId,
      outputPath = qualifiedOutputPath.toString,
      dynamicPartitionOverwrite = dynamicPartitionOverwrite)


    val doInsertion = if (mode == SaveMode.Append) {
      true
    } else {
      val pathExists = fs.exists(finalOutputPath)
      (mode, pathExists) match {
        case (SaveMode.ErrorIfExists, true) =>
          throw new AnalysisException(s"path $finalOutputPath already exists.")
        case (SaveMode.Overwrite, true) =>
          if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
            false
          } else if (dynamicPartitionOverwrite) {
            // For dynamic partition overwrite, do not delete partition directories ahead.
            true
          } else {
            deleteMatchingPartitions(fs, finalOutputPath, customPartitionLocations, committer)
            true
          }
        case (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
          true
        case (SaveMode.Ignore, exists) =>
          !exists
        case (s, exists) =>
          throw new IllegalStateException(s"unsupported save mode $s ($exists)")
      }
    }

    if (doInsertion) {

      def refreshUpdatedPartitions(
          updatedPartitionPaths: Map[String, BasicPartitionStats]): Unit = {
        if (partitionsTrackedByCatalog) {
          val partitionStats =
            updatedPartitionPaths.map(p => PartitioningUtils.parsePathFragment(p._1) -> p._2)
          val updatedPartitions = partitionStats.keySet
          val newPartitions = updatedPartitions -- initialMatchingPartitions
          if (newPartitions.nonEmpty) {
            AlterTableAddPartitionCommand(
              catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
              ifNotExists = true).run(sparkSession)
          }
          // For dynamic partition overwrite, we never remove partitions but only update existing
          // ones.
          if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
            val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
            if (deletedPartitions.nonEmpty) {
              AlterTableDropPartitionCommand(
                catalogTable.get.identifier, deletedPartitions.toSeq,
                ifExists = true, purge = false,
                retainData = true /* already deleted */).run(sparkSession)
            }
          }
          // Update partition stats
          if (SQLConf.get.partitionStatsEnabled) {
            try {
              val updatedPartitionSpec = if (mode == SaveMode.Overwrite) {
                updatedPartitions
              } else {
                newPartitions
              }
              val updatedPartitionStat = sparkSession.sessionState.catalog.listPartitions(
                catalogTable.get.identifier, Some(staticPartitions))
                .filter(p => updatedPartitionSpec.contains(p.spec))
                .map(p => {
                  val stat = partitionStats(p.spec)
                  val newProps = p.parameters ++
                    Map("totalSize" -> stat.numBytes.toString, "numRows" -> stat.numRows.toString)
                  p.copy(parameters = newProps)
                })
              if (updatedPartitionStat.nonEmpty) {
                sparkSession.sessionState.catalog.alterPartitions(catalogTable.get.identifier,
                  updatedPartitionStat)
              }
            } catch {
              case e: Exception => logError("Update partitions statistics error", e)
            }
          }
        }
      }

      // For dynamic partition overwrite, FileOutputCommitter's output path is staging path, files
      // will be renamed from staging path to final output path during commit job
      val committerOutputPath = if (dynamicPartitionOverwrite) {
        FileCommitProtocol.getStagingDir(qualifiedOutputPath.toString, jobId)
          .makeQualified(fs.getUri, fs.getWorkingDirectory)
      } else {
        qualifiedOutputPath
      }


      val updatedPartitionsWithStats =
        FileFormatWriter.write(
          sparkSession = sparkSession,
          plan = child,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(
            committerOutputPath.toString, customPartitionLocations, outputColumns),
          hadoopConf = hadoopConf,
          partitionColumns = partitionColumns,
          bucketSpec = bucketSpec,
          statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
          options = options) - "staticPart"

      var updatedPartitionPaths = updatedPartitionsWithStats.keySet
      logInfo("updated partition paths " + updatedPartitionPaths.mkString(","))
      logInfo("updated partition total " + updatedPartitionPaths.size)
      if (canMerge) {
        updatedPartitionPaths = updatedPartitionPaths.map(p => p.replace("/" + mergeDir, ""))
        try {
          logInfo("enable merge files for " + qualifiedOutputPath)
          val directRenamePathList: ListBuffer[String] = ListBuffer.empty
          val dataAttr = outputColumns.filterNot(AttributeSet(partitionColumns).contains)
          if (partitionColumns.isEmpty) {
            logInfo("non partition insert, merge single dir " + qualifiedOutputPath)
            // scalastyle:off
            val rePartitionNum = MergeUtils.getTargetFileNum(qualifiedOutputPath, hadoopConf, avgConditionSize, outputAverageSize)
            logInfo(s"[mergeFile] static $qualifiedOutputPath rePartionNum is: " + rePartitionNum)
            if (rePartitionNum > 0) {
              val committer2 = FileCommitProtocol.instantiate(
                sparkSession.sessionState.conf.fileCommitProtocolClass,
                jobId = java.util.UUID.randomUUID().toString,
                outputPath = finalOutputPath.toString)
              val sourceRdd = MergeUtils.getRdd(sparkSession, qualifiedOutputPath, dataAttr, fileFormat, options).coalesce(rePartitionNum)
              FileFormatWriter.writeHiveTableRDD(sparkSession, sourceRdd, fileFormat, committer2,
                FileFormatWriter.OutputSpec(finalOutputPath.toString, Map.empty, dataAttr), hadoopConf, Seq.empty,
                statsTrackers = Seq.empty, options)
              logInfo("[mergeFile] merge static dir finished")
            } else {
              logInfo(s"direct rename $qualifiedOutputPath's files")
              val files = fs.listStatus(qualifiedOutputPath)
              files.foreach(file => directRenamePathList+= file.getPath.toString)
            }
          } else {
            logInfo("has partition insert, merge multi dir " + qualifiedOutputPath)
            val tmpDynamicPartInfos = MergeUtils.getTmpDynamicPartPathInfo(fs, qualifiedOutputPath, hadoopConf)
            if (dynamicPartitionOverwrite) {
              MergeUtils.deleteExistingPartition(fs, tmpDynamicPartInfos.keys.toSeq.map(new Path(_)), mergeDir)
            }
            val mergeRule = MergeUtils.generateDynamicMergeRule(fs, qualifiedOutputPath, hadoopConf, avgConditionSize, outputAverageSize, directRenamePathList)
            if (mergeRule.nonEmpty) {
              val committerJobPair = new ArrayBuffer[(FileCommitProtocol, Job)]()
              val rdds = mergeRule.map { r =>
                val finalPartPath = r.path.toString.replace("/" + mergeDir, "")
                logInfo(s"[makeMergedRDDForPartitionedTable] create rdd for ${r.path} to $finalPartPath coalesce ${r.numFiles}")
                val partitionCommitter = FileCommitProtocol.instantiate(
                  sparkSession.sessionState.conf.fileCommitProtocolClass,
                  jobId = java.util.UUID.randomUUID().toString,
                  outputPath = finalPartPath)
                val job = Job.getInstance(hadoopConf)
                job.setOutputKeyClass(classOf[Void])
                job.setOutputValueClass(classOf[InternalRow])
                FileOutputFormat.setOutputPath(job, new Path(finalPartPath))
                committerJobPair += ((partitionCommitter, job))
                MergeUtils.getRdd(sparkSession, new Path(r.path), dataAttr, fileFormat, options).coalesce(r.numFiles)
              }

              for (i <- rdds.indices) {
                val outputDir = committerJobPair(i)._2.getConfiguration.get(FileOutputFormat.OUTDIR)
                logInfo(s"rdd $i correspond with $outputDir")
              }

              // index partition id of unionrdd with repsective committer
              val union = new UnionRDD(rdds.head.context, rdds)
              val rddIndex2Committer = union.rdds.zipWithIndex.map(_._2).zip(committerJobPair.map(pair => (pair._1, pair._2.getConfiguration.get(FileOutputFormat.OUTDIR)))).toMap
              val map4Committer = union.getPartitions.map { p =>
                val up = p.asInstanceOf[UnionPartition[RDD[InternalRow]]]
                val (withCommitter, finalPath) = rddIndex2Committer(up.parentRddIndex)
                logInfo(s"partition index ${up.index} correspond with rdd ${up.parentRddIndex} writer data into ${finalPath}")
                (up.index, (withCommitter, finalPath))
              }.toMap
              FileFormatWriter.writerPartitionHiveTableRDD(sparkSession, union, fileFormat, map4Committer, committerJobPair,
                FileFormatWriter.OutputSpec(
                  "", Map.empty, dataAttr), hadoopConf, Seq.empty, Seq.empty, options)
              logInfo("[mergeFile] merge dynamic partition finished")
            }
          }
          if (!directRenamePathList.isEmpty) {
            directRenamePathList.foreach { path_string =>
              val path = new Path(path_string)
              if (fs.isDirectory(path)) {
                // scalastyle:off
                val (destPath, existed) = MergeUtils.checkPartitionDirExists(fs, path, qualifiedOutputPath, finalOutputPath)
                if (existed) {
                  val files = fs.listStatus(path)
                  files.foreach { f =>
                    if (f.getPath.getName == FileOutputCommitter.SUCCEEDED_FILE_NAME) {
                      if (!fs.rename(f.getPath, destPath))
                        logInfo("skip rename marked success file, maybe already exists")
                    } else {
                      if (!fs.rename(f.getPath, destPath)) throw new IOException(s"direct rename fail from ${f.getPath} to $destPath")
                      logInfo("direct rename file [" + f.getPath + " to " + destPath + "]")
                    }
                  }
                } else {
                  if (!fs.rename(path, destPath)) throw new IOException(s"direct rename fail from $path to $destPath")
                  logInfo("direct rename dir [" + path + " to " + destPath + "]")
                }
              } else {
                if (path.getName == FileOutputCommitter.SUCCEEDED_FILE_NAME) {
                  if (!fs.rename(path, finalOutputPath))
                    logInfo("skip rename marked success file, maybe already exists")
                } else {
                  if (!fs.rename(path, finalOutputPath)) throw new IOException(s"direct rename fail from $path to $finalOutputPath")
                  logInfo("direct rename file [" + path + " to " + finalOutputPath + "]")
                }
              }
            }
          }
        } finally {
          fs.delete(qualifiedOutputPath, true)
          logInfo("delete temp path " + qualifiedOutputPath)
        }
      }

      // update metastore partition metadata
      if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
        && partitionColumns.length == staticPartitions.size) {
        // Avoid empty static partition can't loaded to datasource table.
        val staticPathFragment =
          PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
        refreshUpdatedPartitions(Map(staticPathFragment -> BasicPartitionStats(0L, 0L)))
      } else {
        refreshUpdatedPartitions(updatedPartitionsWithStats)
      }

      // refresh cached files in FileIndex
      fileIndex.foreach(_.refresh())
      // refresh data cache if table is cached
      sparkSession.sharedState.cacheManager.recacheByPath(sparkSession, outputPath, fs)

      if (catalogTable.nonEmpty) {
        CommandUtils.updateTableStats(sparkSession, catalogTable.get)
      }

    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }


  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(
      fs: FileSystem,
      qualifiedOutputPath: Path,
      customPartitionLocations: Map[TablePartitionSpec, String],
      committer: FileCommitProtocol): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name).map(getPartitionPathString(p.name, _))
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
        throw new IOException(s"Unable to clear partition " +
          s"directory $path prior to writing to it")
      }
    }
  }

  /**
   * Given a set of input partitions, returns those that have locations that differ from the
   * Hive default (e.g. /k1=v1/k2=v2). These partitions were manually assigned locations by
   * the user.
   *
   * @return a mapping from partition specs to their custom locations
   */
  private def getCustomPartitionLocations(
      fs: FileSystem,
      table: CatalogTable,
      qualifiedOutputPath: Path,
      partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }
}
