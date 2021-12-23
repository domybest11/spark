
package org.apache.spark.deploy

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}

sealed trait SparkConfRule extends Logging {

  private[spark] val AUTO_SET_ENABLED = "spark.deploy.autoConf"
  private[spark] val APP_RETRY_TIMES = "spark.deploy.appRetryTimes"

  def apply(helper: SparkConfHelper): Unit = {
    try {
      doApply(helper)
      logInfo(s"Try to apply spark conf rule success for rule ${this.getClass.getName}")
    } catch {
      case throwable: Throwable =>
        logWarning(s"Try to apply spark conf rule error for rule ${this.getClass.getName}",
          throwable)
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
      val originalExecutorMemory = sparkConf.getSizeAsMb(config.EXECUTOR_MEMORY.key)
      val preExecutorMemory = helper.getMetricByKey(EXECUTOR_MEMORY)
        .map(_.asInstanceOf[Int]).getOrElse(originalExecutorMemory.toInt)
      val peakExecutorMemory =
        helper.getMetricByKey(PEAK_EXECUTOR_MEMORY).map(_.asInstanceOf[Int]).getOrElse(-1)
      var executorMemory = preExecutorMemory

      if (peakExecutorMemory == 0) {
        executorMemory = math.min(preExecutorMemory, 3072)
        helper.addEffectiveRules(EXECUTOR_MEMORY)
      } else if (peakExecutorMemory > 0) {
        val ratio = peakExecutorMemory.toDouble / preExecutorMemory.toDouble
        if (ratio < 0.3) {
          executorMemory = math.max(math.ceil(preExecutorMemory / 2048).toInt * 1024, 4096)
          helper.addEffectiveRules(EXECUTOR_MEMORY)
        } else if (ratio > 0.8 && preExecutorMemory < 20480) {
          val originalExecutorMemoryOverhead =
            sparkConf.getSizeAsMb(config.EXECUTOR_MEMORY_OVERHEAD.key).toInt
          val capacity = 28672 - originalExecutorMemoryOverhead
          executorMemory = math.min(capacity,
            math.max(preExecutorMemory, peakExecutorMemory) + 1024)
          helper.addEffectiveRules(EXECUTOR_MEMORY)
        }
      }
      logInfo(s"Set Rule ExecutorMemory from ${originalExecutorMemory}m to ${executorMemory}m")
      helper.setConf(config.EXECUTOR_MEMORY.key, s"${executorMemory}m")
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
  private[spark] val EXECUTE_RULES = "rules"
  private[spark] val MERGE_FILES = "MergeFiles"
  private[spark] val REPARTITION_BEFORE_WRITE = "repartitionBeforeWriteTable"

  override def doApply(helper: SparkConfHelper): Unit = {
    if (enabled(sparkConf) && helper.getJobTag().isDefined) {
      val rules = helper.getMetricByKey(EXECUTE_RULES)
        .map(_.asInstanceOf[String]).getOrElse("")
      val numPartOpt = helper.getMetricByKey(PART_NUM)
      if (numPartOpt.isDefined && numPartOpt.map(_.asInstanceOf[Int]).getOrElse(0) > 0 &&
          (rules.contains(MERGE_FILES) || rules.contains(REPARTITION_BEFORE_WRITE))) {
        helper.addEffectiveRules(REPARTITION_BEFORE_WRITE)
        helper.setConf("spark.sql.insertRebalancePartitionsBeforeWrite.enabled", "true")
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

  private[spark] val ENABLE_PUSH_SHUFFLE = "pushShuffleEnabled"
  private[spark] val IS_PUSH_SHUFFLE = "isPushShuffle"

  override def doApply(helper: SparkConfHelper): Unit = {
    if (enabled(sparkConf) && helper.getJobTag().isDefined) {
      val isPushShuffle = helper.getMetricByKey(IS_PUSH_SHUFFLE)
      if (isPushShuffle.isDefined && isPushShuffle.map(String.valueOf(_)).get.toBoolean) {
        val enablePushShuffle = helper.getMetricByKey(ENABLE_PUSH_SHUFFLE)
        if (enablePushShuffle.isDefined &&
          enablePushShuffle.map(String.valueOf(_)).get.toBoolean) {
          helper.setConf("spark.shuffle.push.enabled", "true")
          helper.addEffectiveRules(SHUFFLE_RULE)
          logInfo("Push-based shuffle rule has taken effect.")
        }
      }
    }
  }
}