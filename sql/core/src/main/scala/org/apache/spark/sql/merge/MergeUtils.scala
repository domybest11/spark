
package org.apache.spark.sql.merge

import java.io.IOException
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.metastore.{TableType => HiveTableType}
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Order}
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DDL_TIME
import org.apache.hadoop.hive.ql.exec.{AbstractFileMergeOperator, OrcFileMergeOperator, RCFileMergeOperator, Utilities}
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.hive.ql.io.orc.OrcFileStripeMergeRecordReader
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeRecordReader
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC
import org.apache.hadoop.hive.ql.plan.{FileMergeDesc, OrcFileMergeDesc, RCFileMergeDesc, TableDesc}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileOutputFormat, FileSplit, JobConf, RecordReader}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkContext, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListenerRuleExecute
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, NullType, StructField, StructType}
import org.apache.spark.util.{RpcUtils, SerializableJobConf, Utils}

object MergeUtils extends Logging {

  val ORC = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
  val RC = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat"
  val PARQUET = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  val TEXT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  val TEMP_DIR = "_tmp.-ext-10000"
  val SCHEMA = "mapred.table.schema"
  val SUPPORTED_FORMAT = Set(ORC, RC, PARQUET, TEXT)

  def getMergeFileReader[K, V](outputFormat: String, file: String, conf: Configuration)
  : RecordReader[K, V] = {
    //scalastyle:off
    val fs = FileSystem.get(conf)
    val fileStatus = fs.getFileStatus(new Path(file))
    outputFormat match {
      case ORC =>
        new OrcFileStripeMergeRecordReader(conf,
          new FileSplit(fileStatus.getPath, 0, fileStatus.getLen, Array.empty[String]))
          .asInstanceOf[RecordReader[K, V]]
      case RC =>
        new RCFileBlockMergeRecordReader(conf,
          new FileSplit(fileStatus.getPath, 0, fileStatus.getLen, Array.empty[String]))
          .asInstanceOf[RecordReader[K, V]]
      case PARQUET =>
        new FileRecordReader(file).asInstanceOf[RecordReader[K, V]]
      case TEXT =>
        new FileRecordReader(file).asInstanceOf[RecordReader[K, V]]
    }
  }

  def getFileMergeDesc[T <: FileMergeDesc](outputFormat: String, outputPath: Path): T = {
    val fmd = outputFormat match {
      case ORC =>
        new OrcFileMergeDesc().asInstanceOf[T]
      case RC =>
        new RCFileMergeDesc().asInstanceOf[T]
      case PARQUET =>
        new FileMergeDesc(null, outputPath).asInstanceOf[T]
      case TEXT =>
        new FileMergeDesc(null, outputPath).asInstanceOf[T]
    }
    fmd.setDpCtx(null)
    fmd.setHasDynamicPartitions(false)
    fmd.setListBucketingAlterTableConcatenate(false)
    fmd.setListBucketingDepth(0)
    fmd.setOutputPath(outputPath)
    fmd
  }

  def getFileMergeOperator[T <: FileMergeDesc](outputFormat: String, outputPath: Path,
    conf: Configuration): AbstractFileMergeOperator[T] = {
    val mergeOperator: AbstractFileMergeOperator[T] = outputFormat match {
      case ORC =>
        new OrcFileMergeOperator().asInstanceOf[AbstractFileMergeOperator[T]]
      case RC =>
        new RCFileMergeOperator().asInstanceOf[AbstractFileMergeOperator[T]]
      case PARQUET =>
        val schema = DataType.fromJson(conf.get(SCHEMA)).asInstanceOf[StructType]
        new ParquetFileMergeOperator(conf, schema).asInstanceOf[AbstractFileMergeOperator[T]]
      case TEXT =>
        new TextFileMergeOperator(conf).asInstanceOf[AbstractFileMergeOperator[T]]
    }
    val fmd = getFileMergeDesc[T](outputFormat, outputPath)
    mergeOperator.setConf(fmd)
    mergeOperator
  }

  def commitTask(fs: FileSystem, attemptPath: Path, taskPath: Path, retryWaitMs: Long): Unit = {
    var attempts = 0
    var lastException: Exception = null
    val maxRetries = 3
    while (attempts < maxRetries) {
      attempts += 1
      try {
        if (fs.exists(taskPath)) {
          log.info("[commit merge task] taskPath already exists, taskPath:" + taskPath)
          return
        } else if (fs.rename(attemptPath, taskPath)) {
          log.info("[commit merge task] rename attemptPath to taskPath successfully! attemptPath:"
            + attemptPath + " taskPath:" + taskPath)
          return
        }
      } catch {
        case ie: InterruptedException =>
          log.error("get InterruptedException", ie)
          throw ie
        case e: Exception =>
          lastException = e
          log.error("get Exception", e)
          e.printStackTrace()
      }

      if (attempts < maxRetries) {
        Thread.sleep(retryWaitMs)
      }
    }

    throw new SparkException(
      s"Error rename ${attemptPath.toString}", lastException)
  }

  def getExternalMergeTmpPath(tempPath: Path, hadoopConf: Configuration): Path = {
    val fs = tempPath.getFileSystem(hadoopConf)
    val tempMergePath = tempPath.toString.replace("-ext-10000", TEMP_DIR)
    val dir: Path = fs.makeQualified(new Path(tempMergePath))
    logDebug("Created temp merging dir = " + dir + " for path = " + tempPath)
    try {
      if (!FileUtils.mkdir(fs, dir, true, hadoopConf)) {
        throw new IllegalStateException("Cannot create staging directory  '" + dir.toString + "'")
      }
    } catch {
      case e: IOException =>
        throw new RuntimeException(
          "Cannot create temp merging directory '" + dir.toString + "': " + e.getMessage, e)
    }
    dir
  }


  def getTargetFileNum(path: Path, conf: Configuration,
                       avgConditionSize: Long, targetFileSize: Long): Int = {
    var numFiles = -1
    try {
      val fs = path.getFileSystem(conf)
      if (fs.exists(path)) {
        val averageSize = getPathSize(fs, path)
        numFiles = computeMergePartitionNum(averageSize, avgConditionSize, targetFileSize)
      }
    } catch {
      case e: IOException => log.error("get FileSystem failed!", e)
    }
    numFiles
  }

  class AverageSize(var totalSize: Long, var numFiles: Int, var files: Seq[String]) {
    def getAverageSize: Long = {
      if (numFiles != 0) {
        totalSize / numFiles
      } else {
        0
      }
    }

    override def toString: String = "{totalSize: " + totalSize + ", numFiles: " + numFiles + "}"
  }

  def getPathSize(fs: FileSystem, dirPath: Path): AverageSize = {
    val error = new AverageSize(-1, -1, Seq.empty[String])
    try {
      val fStats = fs.listStatus(dirPath).filter(_.getLen > 0)

      var totalSz: Long = 0L
      var numFiles: Int = 0

      for (fStat <- fStats) {
        if (fStat.isDirectory()) {
          val avgSzDir = getPathSize(fs, fStat.getPath)
          if (avgSzDir.totalSize < 0) {
            return error
          }
          totalSz += avgSzDir.totalSize
          numFiles += avgSzDir.numFiles
        } else {
          if (!fStat.getPath.toString.endsWith("_SUCCESS")) {
            totalSz += fStat.getLen
            numFiles += 1
          }
        }
      }
      new AverageSize(totalSz, numFiles, fStats.map(_.getPath.toString))
    } catch {
      case _: IOException => error
    }
  }

  def computeMergePartitionNum(averageSize: AverageSize, avgConditionSize: Long,
                               targetFileSize: Long): Int = {
    var partitionNum = -1
    if (averageSize.numFiles <= 1) {
      partitionNum = -1
    } else {
      if (averageSize.getAverageSize > avgConditionSize) {
        partitionNum = -1
      } else {
        partitionNum = Math.ceil(averageSize.totalSize.toDouble / targetFileSize).toInt
      }
    }
    partitionNum
  }

  // get all the dynamic partition path and it's file size
  def getTmpDynamicPartPathInfo(
                                 fs: FileSystem,
                                 tmpPath: Path,
                                 conf: Configuration): mutable.Map[String, AverageSize] = {
    val fStatus = fs.listStatus(tmpPath)
    val partPathInfo = mutable.Map[String, AverageSize]()

    for (fStat <- fStatus) {
      if (fStat.isDirectory) {
        logDebug("[TmpDynamicPartPathInfo] path: " + fStat.getPath.toString)
        if (!hasFile(fs, fStat.getPath, conf)) {
          partPathInfo ++= getTmpDynamicPartPathInfo(fs, fStat.getPath, conf)
        } else {
          val avgSize = getPathSize(fs, fStat.getPath)
          logDebug("pathSizeMap: (" + fStat.getPath.toString + " -> " + avgSize.totalSize + ")")
          partPathInfo += (fStat.getPath.toString -> avgSize)
        }
      }
    }
    partPathInfo
  }


  def hasFile(fs: FileSystem, path: Path, conf: Configuration): Boolean = {
    val fStatus = fs.listStatus(path)
    for (fStat <- fStatus) {
      if (fStat.isFile) {
        return true
      }
    }
    false
  }

  case class DynamicMergeRule(path: String, files: Seq[String], numFiles: Int)

  def generateDynamicMergeRule(
     fs: FileSystem,
     path: Path,
     conf: Configuration,
     avgConditionSize: Long,
     targetFileSize: Long,
     directRenamePathList: ListBuffer[String]): Seq[DynamicMergeRule] = {
    val tmpDynamicPartInfos = getTmpDynamicPartPathInfo(fs, path, conf)
    logDebug("[generateDynamicMergeRule] partInfo size: " + tmpDynamicPartInfos.size)
    val avgMergeSize: Long = if (!tmpDynamicPartInfos.isEmpty) {
      tmpDynamicPartInfos.map(part => part._2.totalSize).sum / tmpDynamicPartInfos.size
    } else {
      0L
    }
    logDebug("[generateDynamicMergeRule] avgMergeSize -> " + avgMergeSize)
    val mergeRule = new ArrayBuffer[DynamicMergeRule]()
    if (avgMergeSize > 0) {
      for (part <- tmpDynamicPartInfos) {
        // if the average file size is greater than the target merge size,
        // just move it to temp merge path
        if (part._2.numFiles <= 1 || part._2.getAverageSize > avgConditionSize) {
          logInfo("mark to rename [" + part._1.toString + " to "
            + part._1.toString.replace("-ext-10000", TEMP_DIR)
            + "]; number of files under " + part._1.toString + " is: " + part._2.numFiles)
          directRenamePathList += part._1
        } else {
          val numFiles = computeMergePartitionNum(part._2, avgConditionSize, targetFileSize)
          mergeRule += DynamicMergeRule(part._1, part._2.files, numFiles)
        }
      }
    }
    mergeRule
  }

  def getRdd(
     sparkSession: SparkSession,
     input: Path,
     dataAttr: Seq[Attribute],
     fileFormat: FileFormat,
     options: Map[String, String]): RDD[InternalRow] = {
    val fi = new InMemoryFileIndex(sparkSession, Seq(input), Map.empty, None)
    val relation = HadoopFsRelation(fi, new StructType(), dataAttr.toStructType, None, fileFormat, options)(sparkSession)
    val fsScanExec = FileSourceScanExec(relation, dataAttr, dataAttr.toStructType, Seq.empty, None, None, Seq.empty, None)
    QueryExecution.prepareExecutedPlan(sparkSession, fsScanExec).execute()
  }

  def deleteExistingPartition(fs: FileSystem, paths: Seq[Path], mergedir: String): Unit = {
    paths.foreach { p =>
      val finalPartPath = new Path(p.toString.replace("/" + mergedir, ""))
      if (fs.exists(finalPartPath)) {
        fs.delete(finalPartPath, true)
        logInfo("delete overwrite dir " + finalPartPath)
      }
    }
  }

  //compatible with insert into/overwrite case that the dest dir handle
  def checkPartitionDirExists(fs: FileSystem, tempPath: Path, qualifiedPath: Path, finalOutputPath: Path): (Path, Boolean) = {
    val tempJavaPath = Paths.get(tempPath.toUri.getPath)
    val qualifiedJavaPath = Paths.get(qualifiedPath.toUri.getPath)
    val relativeJavaPath = qualifiedJavaPath.relativize(tempJavaPath)
    val finalDestPath = new Path(finalOutputPath, relativeJavaPath.toString)
    val finalDestParent = finalDestPath.getParent
    if (!fs.exists(finalDestParent)) {
      fs.mkdirs(finalDestParent)
      logInfo("mkdir direct parent " + finalDestParent)
    }
    if (fs.exists(finalDestPath)) {
      logInfo(s"existing dest dir $finalDestPath")
      (finalDestPath, true)
    } else {
      logInfo(s"non existing dest dir $finalDestPath")
      (finalDestPath, false)
    }
  }


  def mergePathRDD(
     sc: SparkContext,
     files: Array[(String, Array[String])],
     numFiles: Int): RDD[(String, Array[String])] = {
    sc.parallelize(files, numFiles)
  }

  def mergeAction(
     conf: SerializableJobConf,
     outputClassName: String,
     files: Array[String],
     outputDir: String,
     tmpMergeLocationDir: String,
     extension: String,
     waitTime: Long): Unit = {
    val jobConf = conf.value
    val context = TaskContext.get()
    val taskId = "_" + "%06d".format(context.partitionId()) + "_" + context.attemptNumber()
    val fs = FileSystem.get(jobConf)
    //hive tmpdir:/-ext-10000/a/b/file   spark tmpdir:/a/b/file
    val tmpMergeLocationPath = new Path(tmpMergeLocationDir)
    if (!fs.exists(tmpMergeLocationPath)) {
      fs.mkdirs(tmpMergeLocationPath)
    }
    val attemptPath = new Path(tmpMergeLocationDir + "/" + taskId)
    val outputPath = new Path(outputDir)
    val taskTmpPath = new Path(outputDir.replace("/-ext-10000", "/_task_tmp.-ext-10000"))
    jobConf.set("mapred.task.id", taskId)
    jobConf.set("mapred.task.path", attemptPath.toString)
    val mergeOp = getFileMergeOperator[FileMergeDesc](outputClassName, outputPath, jobConf)
    mergeOp.initializeOp(jobConf)
    val pClass = outputPath.getClass
    val updatePathsMethod = mergeOp.getClass.getSuperclass
      .getDeclaredMethod("updatePaths", pClass, pClass)
    updatePathsMethod.setAccessible(true)
    updatePathsMethod.invoke(mergeOp, new Path(tmpMergeLocationDir), taskTmpPath)
    val row = new Array[AnyRef](2)
    files.foreach { file =>
      val reader = getMergeFileReader[AnyRef, AnyRef](outputClassName,
        file, jobConf)
      var key = reader.createKey()
      var value = reader.createValue()
      try {
        while (reader.next(key, value)) {
          row(0) = key
          row(1) = value
          mergeOp.process(row, 0)
        }
      } catch {
        case e: HiveException =>
          throw new IOException(e)
      } finally {
        reader.close()
      }
    }
    mergeOp.closeOp(false)
    fs.cancelDeleteOnExit(outputPath)
    val taskPath = new Path(tmpMergeLocationDir + "/part-" +
      "%05d".format(context.partitionId()) + extension)
    commitTask(fs, attemptPath, taskPath, waitTime)
  }


  def mergeFile(
     path: Path,
     fs: FileSystem,
     fileSinkConf: FileSinkDesc,
     conf: SerializableJobConf,
     directRenamePathList: ListBuffer[String],
     speculationEnabled: Boolean,
     existDynamicPartition: Boolean,
     retryWaitMs: Long,
     avgConditionSize: Long,
     targetFileSize: Long,
     sparkSession: SparkSession): Unit = {
    val sparkContext = sparkSession.sparkContext
    val hiveOutputFormat: HiveOutputFormat[AnyRef, Writable] = conf.value.getOutputFormat
      .asInstanceOf[HiveOutputFormat[AnyRef, Writable]]
    val extension = Utilities.getFileExtension(conf.value,
      fileSinkConf.compressed, hiveOutputFormat)
    val outputClassName = fileSinkConf.tableInfo.getOutputFileFormatClassName
    val outputDir = path.toString
    var tmpMergeLocation = MergeUtils.getExternalMergeTmpPath(path, conf.value)
    val tmpMergeLocationDir = tmpMergeLocation.toString
    fileSinkConf.dir = tmpMergeLocation.toString
    val waitTime = retryWaitMs

    if (existDynamicPartition) {
      val mergeRules = MergeUtils.generateDynamicMergeRule(fs, path,
        conf.value, avgConditionSize, targetFileSize, directRenamePathList)
      if (mergeRules.nonEmpty) {
        sparkContext.listenerBus.post(SparkListenerRuleExecute("MergeFiles"))
      }
      sparkContext.union(mergeRules.map { r =>
        val groupSize = Math.ceil(r.files.size * 1d / r.numFiles).toInt
        val groupedFiles = r.files.toArray.grouped(groupSize).map(x => (r.path, x)).toArray
        MergeUtils.mergePathRDD(sparkContext, groupedFiles, groupedFiles.size)
      }).foreach { case (partOutputDir, files) =>
        val tmpPartMergeLocationDir = partOutputDir.replace("-ext-10000", MergeUtils.TEMP_DIR)
        MergeUtils.mergeAction(conf, outputClassName, files, partOutputDir, tmpPartMergeLocationDir,
          extension, waitTime)
      }
      if (speculationEnabled) {
        mergeRules.foreach { r =>
          val specFiles = fs.listStatus(
            new Path(r.path.toString.replace("-ext-10000", MergeUtils.TEMP_DIR)))
            .filter(!_.getPath.getName.startsWith("part"))
          specFiles.foreach(f => {
            logInfo("delete speculative task output temp file, path:" +
              f.getPath)
            fs.delete(f.getPath)
          })
        }
      }
    } else {
      val numFiles = MergeUtils.getTargetFileNum(path, conf.value,
        avgConditionSize, targetFileSize)
      if (numFiles > 0) {
        val files = fs.listStatus(path).filter(_.getLen > 0).map(_.getPath.toString)
        val groupSize = Math.ceil(files.size * 1d / numFiles).toInt
        val groupedFiles = files.grouped(groupSize).toArray
        if (groupedFiles.nonEmpty && groupedFiles.size > 0) {
          sparkContext.listenerBus.post(SparkListenerRuleExecute("MergeFiles"))
        }
        fileSinkConf.dir = tmpMergeLocation.toString
        sparkContext.parallelize(groupedFiles, groupedFiles.size).foreach { files =>
          MergeUtils.mergeAction(conf, outputClassName, files, outputDir, tmpMergeLocationDir,
            extension, waitTime)
        }
        if (speculationEnabled) {
          val specFiles = fs.listStatus(tmpMergeLocation)
            .filter(!_.getPath.getName.startsWith("part"))
          specFiles.foreach(f => fs.delete(f.getPath))
        }
        if (conf.value.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)) {
          fs.createNewFile(new Path(tmpMergeLocationDir + "/_SUCCESS"))
        }
      } else {
        tmpMergeLocation = path
      }
    }
    FileOutputFormat.setOutputPath(conf.value, tmpMergeLocation)
  }


  def mergeHiveTableOutput(
     sparkSession: SparkSession,
     table: CatalogTable,
     tmpLocation: Path,
     hadoopConf: Configuration,
     existDynamicPartition: Boolean): JobConf = {
    val jobConf = new JobConf(hadoopConf)
    val hiveQlTable = toHiveTable(table)
    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
      // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
      // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
      // HiveSequenceFileOutputFormat.
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )
    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)
    val speculationEnabled = sparkSession.sparkContext.conf.getBoolean("spark.speculation", false)
    val outputFormatClass = fileSinkConf.tableInfo.getOutputFileFormatClassName
    val avgConditionSize: Long = sparkSession.conf.get(SQLConf.MERGE_SMALLFILE_SIZE)
    val outputAverageSize = sparkSession.conf.get(SQLConf.MERGE_FILE_PER_TASK)
    val retryWaitMs: Long = RpcUtils.retryWaitMs(sparkSession.sparkContext.conf)
    val mergeHiveFiles = sparkSession.sessionState.conf.mergeHiveFiles
    val targetFileSize = Math.max(avgConditionSize, outputAverageSize)
    jobConf.setOutputFormat(fileSinkConf.tableInfo.getOutputFileFormatClass)
    jobConf.set(MergeUtils.SCHEMA, table.dataSchema.json)
    FileOutputFormat.setOutputPath(jobConf, tmpLocation)
    if (mergeHiveFiles && targetFileSize > 0 &&
      MergeUtils.SUPPORTED_FORMAT.contains(outputFormatClass)) {
      val directRenamePathList = ListBuffer.empty[String]
      val rollbackPathList = ListBuffer.empty[String]
      val fs = tmpLocation.getFileSystem(hadoopConf)
      try {
        val ignoreLocalWriter = sparkSession.conf.get(SQLConf.MERGE_FILES_IGNORE_LOCAL_WRITE)
        if (ignoreLocalWriter) {
          jobConf.setBoolean("fs.hdfs.impl.disable.cache", true)
          jobConf.setBoolean("fs.viewfs.impl.disable.cache", true)
          jobConf.setBoolean("dfs.client.avoid.local.write", true)
        }
        mergeFile(tmpLocation, fs, fileSinkConf, new SerializableJobConf(jobConf),
          directRenamePathList, speculationEnabled, existDynamicPartition, retryWaitMs,
          avgConditionSize, targetFileSize, sparkSession)
        if (directRenamePathList.nonEmpty) {
          directRenamePathList.foreach { path =>
            val destPathStr = path.replace("-ext-10000", MergeUtils.TEMP_DIR)
            rollbackPathList+= path
            logInfo("rename [" + path + " to " + destPathStr + "]")
            val destPath = new Path(destPathStr)
            if (!fs.exists(destPath.getParent)) {
              fs.mkdirs(destPath.getParent)
            }
            fs.rename(new Path(path), destPath)
          }
        }
      } catch {
        case ex: Exception =>
          logInfo("Merge file of " + tmpLocation + " failed!", ex)
          fileSinkConf.dir = tmpLocation.toString
          FileOutputFormat.setOutputPath(jobConf, tmpLocation)
          if (rollbackPathList.nonEmpty) {
            rollbackPathList.foreach { path =>
              val srcPath = path.replace("-ext-10000", MergeUtils.TEMP_DIR)
              logInfo("rename [" + srcPath + " to "
                + path + "]")
              fs.rename(new Path(srcPath), new Path(path))
            }
          }
      }
    }
    jobConf
  }


  private class FileSinkDesc(
      var dir: String,
      var tableInfo: TableDesc,
      var compressed: Boolean)
    extends Serializable with Logging {
    var compressCodec: String = _
    var compressType: String = _
    var destTableId: Int = _

    def setCompressed(compressed: Boolean): Unit = {
      this.compressed = compressed
    }

    def getDirName(): String = dir

    def setDestTableId(destTableId: Int): Unit = {
      this.destTableId = destTableId
    }

    def setTableInfo(tableInfo: TableDesc): Unit = {
      this.tableInfo = tableInfo
    }

    def setCompressCodec(intermediateCompressorCodec: String): Unit = {
      compressCodec = intermediateCompressorCodec
    }

    def setCompressType(intermediateCompressType: String): Unit = {
      compressType = intermediateCompressType
    }
  }


  /**
   * Converts the native table metadata representation format CatalogTable to Hive's Table.
   */
  private def toHiveTable(table: CatalogTable, userName: Option[String] = None): HiveTable = {
    case object HiveVoidType extends DataType {
      override def defaultSize: Int = 1

      override def asNullable: DataType = HiveVoidType

      override def simpleString: String = "void"

      def replaceVoidType(dt: DataType): DataType = dt match {
        case ArrayType(et, nullable) =>
          ArrayType(replaceVoidType(et), nullable)
        case MapType(kt, vt, nullable) =>
          MapType(replaceVoidType(kt), replaceVoidType(vt), nullable)
        case StructType(fields) =>
          StructType(fields.map { field =>
            field.copy(dataType = replaceVoidType(field.dataType))
          })
        case _: NullType => HiveVoidType
        case _ => dt
      }
    }

    object HiveExternalCatalog {
      val SPARK_SQL_PREFIX = "spark.sql."
      val SERIALIZATION_FORMAT = "serialization.format"

      val DATASOURCE_PREFIX = SPARK_SQL_PREFIX + "sources."
      val DATASOURCE_PROVIDER = DATASOURCE_PREFIX + "provider"
      val DATASOURCE_SCHEMA = DATASOURCE_PREFIX + "schema"
      val DATASOURCE_SCHEMA_PREFIX = DATASOURCE_SCHEMA + "."
      val DATASOURCE_SCHEMA_NUMPARTS = DATASOURCE_SCHEMA_PREFIX + "numParts"
      val DATASOURCE_SCHEMA_NUMPARTCOLS = DATASOURCE_SCHEMA_PREFIX + "numPartCols"
      val DATASOURCE_SCHEMA_NUMSORTCOLS = DATASOURCE_SCHEMA_PREFIX + "numSortCols"
      val DATASOURCE_SCHEMA_NUMBUCKETS = DATASOURCE_SCHEMA_PREFIX + "numBuckets"
      val DATASOURCE_SCHEMA_NUMBUCKETCOLS = DATASOURCE_SCHEMA_PREFIX + "numBucketCols"
      val DATASOURCE_SCHEMA_PART_PREFIX = DATASOURCE_SCHEMA_PREFIX + "part."
      val DATASOURCE_SCHEMA_PARTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "partCol."
      val DATASOURCE_SCHEMA_BUCKETCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "bucketCol."
      val DATASOURCE_SCHEMA_SORTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "sortCol."

      val STATISTICS_PREFIX = SPARK_SQL_PREFIX + "statistics."
      val STATISTICS_TOTAL_SIZE = STATISTICS_PREFIX + "totalSize"
      val STATISTICS_NUM_ROWS = STATISTICS_PREFIX + "numRows"
      val STATISTICS_COL_STATS_PREFIX = STATISTICS_PREFIX + "colStats."

      val TABLE_PARTITION_PROVIDER = SPARK_SQL_PREFIX + "partitionProvider"
      val TABLE_PARTITION_PROVIDER_CATALOG = "catalog"
      val TABLE_PARTITION_PROVIDER_FILESYSTEM = "filesystem"

      val CREATED_SPARK_VERSION = SPARK_SQL_PREFIX + "create.version"

      val HIVE_GENERATED_TABLE_PROPERTIES = Set(DDL_TIME)
      val HIVE_GENERATED_STORAGE_PROPERTIES = Set(SERIALIZATION_FORMAT)

      // When storing data source tables in hive metastore, we need to set data schema to empty if the
      // schema is hive-incompatible. However we need a hack to preserve existing behavior. Before
      // Spark 2.0, we do not set a default serde here (this was done in Hive), and so if the user
      // provides an empty schema Hive would automatically populate the schema with a single field
      // "col". However, after SPARK-14388, we set the default serde to LazySimpleSerde so this
      // implicit behavior no longer happens. Therefore, we need to do it in Spark ourselves.
      val EMPTY_DATA_SCHEMA = new StructType()
        .add("col", "array<string>", nullable = true, comment = "from deserializer")

      // A persisted data source table always store its schema in the catalog.
      private def getSchemaFromTableProperties(metadata: CatalogTable): StructType = {
        val errorMessage = "Could not read schema from the hive metastore because it is corrupted."
        val props = metadata.properties
        val schema = props.get(DATASOURCE_SCHEMA)
        if (schema.isDefined) {
          // Originally, we used `spark.sql.sources.schema` to store the schema of a data source table.
          // After SPARK-6024, we removed this flag.
          // Although we are not using `spark.sql.sources.schema` any more, we need to still support.
          DataType.fromJson(schema.get).asInstanceOf[StructType]
        } else if (props.filterKeys(_.startsWith(DATASOURCE_SCHEMA_PREFIX)).isEmpty) {
          // If there is no schema information in table properties, it means the schema of this table
          // was empty when saving into metastore, which is possible in older version(prior to 2.1) of
          // Spark. We should respect it.
          new StructType()
        } else {
          val numSchemaParts = props.get(DATASOURCE_SCHEMA_NUMPARTS)
          if (numSchemaParts.isDefined) {
            val parts = (0 until numSchemaParts.get.toInt).map { index =>
              val part = metadata.properties.get(s"$DATASOURCE_SCHEMA_PART_PREFIX$index").orNull
              if (part == null) {
                throw new AnalysisException(errorMessage +
                  s" (missing part $index of the schema, ${numSchemaParts.get} parts are expected).")
              }
              part
            }
            // Stick all parts back to a single schema string.
            DataType.fromJson(parts.mkString).asInstanceOf[StructType]
          } else {
            throw new AnalysisException(errorMessage)
          }
        }
      }

      private def getColumnNamesByType(
         props: Map[String, String],
         colType: String,
         typeName: String): Seq[String] = {
        for {
          numCols <- props.get(s"spark.sql.sources.schema.num${colType.capitalize}Cols").toSeq
          index <- 0 until numCols.toInt
        } yield props.getOrElse(
          s"$DATASOURCE_SCHEMA_PREFIX${colType}Col.$index",
          throw new AnalysisException(
            s"Corrupted $typeName in catalog: $numCols parts expected, but part $index is missing."
          )
        )
      }

      private def getPartitionColumnsFromTableProperties(metadata: CatalogTable): Seq[String] = {
        getColumnNamesByType(metadata.properties, "part", "partitioning columns")
      }

      private def getBucketSpecFromTableProperties(metadata: CatalogTable): Option[BucketSpec] = {
        metadata.properties.get(DATASOURCE_SCHEMA_NUMBUCKETS).map { numBuckets =>
          BucketSpec(
            numBuckets.toInt,
            getColumnNamesByType(metadata.properties, "bucket", "bucketing columns"),
            getColumnNamesByType(metadata.properties, "sort", "sorting columns"))
        }
      }

      /**
       * Detects a data source table. This checks both the table provider and the table properties,
       * unlike DDLUtils which just checks the former.
       */
      private[spark] def isDatasourceTable(table: CatalogTable): Boolean = {
        val provider = table.provider.orElse(table.properties.get(DATASOURCE_PROVIDER))
        provider.isDefined && provider != Some(DDLUtils.HIVE_PROVIDER)
      }

    }

    def toHiveTableType(catalogTableType: CatalogTableType): HiveTableType = {
      catalogTableType match {
        case CatalogTableType.EXTERNAL => HiveTableType.EXTERNAL_TABLE
        case CatalogTableType.MANAGED => HiveTableType.MANAGED_TABLE
        case CatalogTableType.VIEW => HiveTableType.VIRTUAL_VIEW
        case t =>
          throw new IllegalArgumentException(
            s"Unknown table type is found at toHiveTableType: $t")
      }
    }

    def toHiveColumn(c: StructField): FieldSchema = {
      // For Hive Serde, we still need to to restore the raw type for char and varchar type.
      // When reading data in parquet, orc, or avro file format with string type for char,
      // the tailing spaces may lost if we are not going to pad it.
      val typeString = CharVarcharUtils.getRawTypeString(c.metadata)
        .getOrElse(HiveVoidType.replaceVoidType(c.dataType).catalogString)
      new FieldSchema(c.name, typeString, c.getComment().orNull)
    }

    def toInputFormat(name: String) =
      Utils.classForName[org.apache.hadoop.mapred.InputFormat[_, _]](name)

    def toOutputFormat(name: String) =
      Utils.classForName[org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]](name)

    var user: Option[String] = userName
    if (SessionState.getSessionConf.getBoolean("spark.proxyuser.enabled", false)) {
      user = Option.apply(UserGroupInformation.getCurrentUser.getShortUserName)
    }
    val hiveTable = new HiveTable(table.database, table.identifier.table)
    hiveTable.setTableType(toHiveTableType(table.tableType))
    // For EXTERNAL_TABLE, we also need to set EXTERNAL field in the table properties.
    // Otherwise, Hive metastore will change the table to a MANAGED_TABLE.
    // (metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L1095-L1105)
    if (table.tableType == CatalogTableType.EXTERNAL) {
      hiveTable.setProperty("EXTERNAL", "TRUE")
    }
    // Note: In Hive the schema and partition columns must be disjoint sets
    val (partCols, schema) = table.schema.map(toHiveColumn).partition { c =>
      table.partitionColumnNames.contains(c.getName)
    }
    hiveTable.setFields(schema.asJava)
    hiveTable.setPartCols(partCols.asJava)
    Option(table.owner).filter(_.nonEmpty).orElse(user).foreach(hiveTable.setOwner)
    hiveTable.setCreateTime(MILLISECONDS.toSeconds(table.createTime).toInt)
    hiveTable.setLastAccessTime(MILLISECONDS.toSeconds(table.lastAccessTime).toInt)
    table.storage.locationUri.map(CatalogUtils.URIToString).foreach { loc =>
      hiveTable.getTTable.getSd.setLocation(loc)
    }
    table.storage.inputFormat.map(toInputFormat).foreach(hiveTable.setInputFormatClass)
    table.storage.outputFormat.map(toOutputFormat).foreach(hiveTable.setOutputFormatClass)
    hiveTable.setSerializationLib(
      table.storage.serde.getOrElse("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    table.storage.properties.foreach { case (k, v) => hiveTable.setSerdeParam(k, v) }
    table.properties.foreach { case (k, v) => hiveTable.setProperty(k, v) }
    table.comment.foreach { c => hiveTable.setProperty("comment", c) }
    // Hive will expand the view text, so it needs 2 fields: viewOriginalText and viewExpandedText.
    // Since we don't expand the view text, but only add table properties, we map the `viewText` to
    // the both fields in hive table.
    table.viewText.foreach { t =>
      hiveTable.setViewOriginalText(t)
      hiveTable.setViewExpandedText(t)
    }

    table.bucketSpec match {
      case Some(bucketSpec) if !HiveExternalCatalog.isDatasourceTable(table) =>
        hiveTable.setNumBuckets(bucketSpec.numBuckets)
        hiveTable.setBucketCols(bucketSpec.bucketColumnNames.toList.asJava)

        if (bucketSpec.sortColumnNames.nonEmpty) {
          hiveTable.setSortCols(
            bucketSpec.sortColumnNames
              .map(col => new Order(col, HIVE_COLUMN_ORDER_ASC))
              .toList
              .asJava
          )
        }
      case _ =>
    }

    hiveTable
  }


}
