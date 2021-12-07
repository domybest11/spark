
package org.apache.spark.deploy

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.REPARTITION_WRITE_FILE_SIZE_RATIO
import org.apache.spark.internal.{Logging, config}

sealed trait SparkConfRule extends Logging {

  private[spark] val AUTO_SET_ENABLED = "spark.deploy.autoConf"
  private[spark] val APP_RETRY_TIMES = "spark.deploy.appRetryTimes"

  def apply(helper: SparkConfHelper): Unit = {
    try {
      doApply(helper)
      logInfo(s"Try to apply spark conf rule success for rule ${this.getClass.getName}")
    } catch {
      case throwable: Throwable =>
        logWarning(s"Try to apply spark conf rule error for rule ${this.getClass.getName}", throwable)
        fallback(helper)
    }
  }

  def enabled(sparkConf: SparkConf): Boolean = {
    sparkConf.getBoolean(AUTO_SET_ENABLED, false) &&
      sparkConf.getInt(APP_RETRY_TIMES, -1) == 0
  }

  def doApply(helper: SparkConfHelper): Unit

  def fallback(helper: SparkConfHelper): Unit = {

  }
}

case class ExecutorMemoryRule(sparkConf: SparkConf) extends SparkConfRule {

  private[spark] val EXECUTOR_MEMORY = "executorMemory"

  private[spark] val PEAK_EXECUTOR_MEMORY = "peakExecutorMemory"
  private[spark] val PEAK_EXECUTOR_MEMORY_OVERHEAD = "peakExecutorMemoryOverhead"

//  implicit val formats = DefaultFormats

  override def doApply(helper: SparkConfHelper): Unit = {
    if (enabled(sparkConf) && helper.getJobTag().isDefined) {
      val metrics = helper.getMetricByKey(PEAK_EXECUTOR_MEMORY)
      if (metrics != null) {
        val peakExecutorMemory = metrics.map(_.asInstanceOf[Int]).getOrElse(-1)
        val originalExecutorMemory = sparkConf.getSizeAsMb(config.EXECUTOR_MEMORY.key)
        var executorMemory = originalExecutorMemory

        if (peakExecutorMemory == 0) {
          executorMemory = math.min(originalExecutorMemory, 3072)
          helper.addEffectiveRules(EXECUTOR_MEMORY)
        }
        if (peakExecutorMemory > 0) {
          val rate = peakExecutorMemory.toDouble / originalExecutorMemory.toDouble
          if (rate < 0.3) {
            executorMemory = math.max(math.ceil(originalExecutorMemory / 2).toLong, 4096)
            helper.addEffectiveRules(EXECUTOR_MEMORY)
          } else if (rate > 0.8 && originalExecutorMemory < 20480) {
            val originalExecutorMemoryOverhead =
              sparkConf.getSizeAsMb(config.EXECUTOR_MEMORY_OVERHEAD.key)
            val capacity = 28672 - originalExecutorMemoryOverhead
            executorMemory = math.min(capacity,
              math.max(originalExecutorMemory, peakExecutorMemory) + 1024)
            helper.addEffectiveRules(EXECUTOR_MEMORY)
          }
        }
        logInfo(s"Set Rule ExecutorMemory from ${originalExecutorMemory}m to ${executorMemory}m")
        helper.setConf(config.EXECUTOR_MEMORY.key, s"${executorMemory}m")
      }
    }
  }
}

case class AllocationRatioRule(sparkConf: SparkConf) extends SparkConfRule {

  private[spark] val PEAK_ELAPSE_TIME = "costTime"

  private[spark] val EXECUTOR_ALLOCATION_RATIO = "executorAllocationRatio"

  override def doApply(helper: SparkConfHelper): Unit = {
    if (enabled(sparkConf) && helper.getJobTag().isDefined) {
      val metrics = helper.getMetricByKey(PEAK_ELAPSE_TIME)
      val originalAllocationRatio = sparkConf.get(
        config.DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO.key)
      var allocationRatio = originalAllocationRatio
      if (metrics != null) {
        val peakElapsedTime = metrics.map(_.asInstanceOf[Int]).getOrElse(-1)
        logInfo(s"Get history job cost time $peakElapsedTime")
        if (peakElapsedTime > 300000) {
          helper.addEffectiveRules(EXECUTOR_ALLOCATION_RATIO)
          allocationRatio = "1.0"
        }
      }
      logInfo(s"Set Rule ExecutorAllocationRatio from $originalAllocationRatio to $allocationRatio")
      helper.setConf(config.DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO.key, s"${allocationRatio}")
    }
  }
}

case class RepartitionBeforeWriteTableRule(sparkConf: SparkConf) extends SparkConfRule {

  private[spark] val PART_NUM = "numPart"
  private[spark] val FILE_WRITE_COUNT = "numFilesWrite"
  private[spark] val FILE_MERGE_COUNT = "mergeNumFiles"
  private[spark] val WRITE_BYTES = "filesSizeWrite"
  private[spark] val EXECUTE_RULES = "rules"

  private[spark] val REPARTITION_BEFORE_WRITE = "repartitionBeforeWriteTable"

  override def doApply(helper: SparkConfHelper): Unit = {
    if (enabled(sparkConf) && helper.getJobTag().isDefined) {
      val smallest = sparkConf.get("spark.sql.hive.merge.smallfile.size").toLong
      val biggest = sparkConf.get("spark.sql.hive.merge.size.per.task").toLong
      val numPartOpt = helper.getMetricByKey(PART_NUM)
      if (numPartOpt.isDefined && numPartOpt.map(_.asInstanceOf[Int]).getOrElse(-1) < 2) {
        val rules = helper.getMetricByKey(EXECUTE_RULES)
          .map(_.asInstanceOf[String]).getOrElse("")
        val mergeNumFilesOpt = helper.getMetricByKey(FILE_MERGE_COUNT)
        val mergeNumFiles = mergeNumFilesOpt.map(_.asInstanceOf[Int]).getOrElse(-1)
        if (mergeNumFiles > 0) {
          helper.addEffectiveRules(REPARTITION_BEFORE_WRITE)
          helper.setConf("spark.sql.optimizer.insertRepartitionBeforeWrite.enabled", "true")
          helper.setConf("spark.sql.optimizer.insertRepartitionNum", s"${mergeNumFiles}")
        } else if (rules.contains(REPARTITION_BEFORE_WRITE)) {
          val oldNumFilesWrite = helper.getMetricByKey(FILE_WRITE_COUNT)
            .map(String.valueOf(_)).getOrElse("0").toLong
          val filesSizeWrite = helper.getMetricByKey(WRITE_BYTES)
            .map(String.valueOf(_)).getOrElse("0.0").toDouble
          val oldAvgSizeByte = filesSizeWrite / oldNumFilesWrite
          val ratio = sparkConf.getDouble(REPARTITION_WRITE_FILE_SIZE_RATIO.key, 1.0)
          if (oldAvgSizeByte < biggest * ratio) {
            var newNumFilesWrite = oldNumFilesWrite
            if (filesSizeWrite < biggest * 1.2) {
              newNumFilesWrite = 1
            } else {
              val down = Math.ceil(filesSizeWrite / biggest)
              val up = Math.floor(filesSizeWrite / smallest)
              newNumFilesWrite = Math.round(down + (up - down) * 0.25)
            }
            helper.addEffectiveRules(REPARTITION_BEFORE_WRITE)
            helper.setConf("spark.sql.optimizer.insertRepartitionBeforeWrite.enabled", "true")
            helper.setConf("spark.sql.optimizer.insertRepartitionNum", s"${newNumFilesWrite}")
          }
        }
      }
    }
  }
}

case class DataSourceGrayScaleRelease(sparkConf: SparkConf) extends SparkConfRule {

  private[spark] val DATA_SOURCE = "dataSource"

  override def doApply(helper: SparkConfHelper): Unit = {
    val grayLevel = sparkConf.getInt("spark.deploy.grayLevel", -1)
    var autoSet = false

    val appName = sparkConf.getOption("spark.app.name")
    if (enabled(sparkConf) && grayLevel > 0 && appName.getOrElse("").startsWith("a_h")) {
      val jobId = appName.get.split("_")(3)
      if (jobId.toLong % 100 < grayLevel) {
        autoSet = true
      }
    }
    if (autoSet || sparkConf.getBoolean("spark.sql.test.mirrorExecute", false)) {
      helper.setConf("spark.sql.hive.convertMetastoreParquet", "true")
      helper.setConf("spark.sql.hive.convertMetastoreOrc", "true")
      helper.setConf("spark.sql.legacy.createHiveTableByDefault", "false")
      helper.setConf("spark.sql.sources.default", "orc")
      helper.addEffectiveRules(DATA_SOURCE)
      logInfo("Rule DataSourceGrayScaleRelease took effect")
    }
  }
}

case class PushShuffleRule(sparkConf: SparkConf) extends SparkConfRule {
  private[spark] val SHUFFLE_RULE = "pushBasedShuffleRule"

  private[spark] val NUM_PARTITIONS = "numPartitions"
  private[spark] val IS_PUSH_SHUFFLE = "isPushShuffle"

  override def doApply(helper: SparkConfHelper): Unit = {
    if (enabled(sparkConf) && helper.getJobTag().isDefined) {
      val isPushShuffle = helper.getMetricByKey(IS_PUSH_SHUFFLE)
      if (isPushShuffle.isDefined && isPushShuffle.map(String.valueOf(_)).get.toBoolean) {
        val numPartitions = helper.getMetricByKey(NUM_PARTITIONS)
        if (numPartitions.isDefined &&
          numPartitions.map(String.valueOf(_)).getOrElse("-1").toLong > 0) {
          val targetNumPartitions = numPartitions.map(String.valueOf(_)).getOrElse("-1").toLong
          helper.setConf("spark.sql.shuffle.partitions", s"${targetNumPartitions}")
          helper.setConf("spark.shuffle.push.enabled", "true")
          helper.addEffectiveRules(SHUFFLE_RULE)
          logInfo("Shuffle rule has taken effect.")
        }
      }
    }
  }
}