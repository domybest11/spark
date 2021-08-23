
package org.apache.spark.deploy

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.HttpClientUtils

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
    val jobTag = sparkConf.getOption("spark.deploy.jobTag")
    if (enabled(sparkConf) && jobTag.isDefined) {
      val metrics = HttpClientUtils.getInstance().getJobHistoryMetric(jobTag.get).asScala
      if (metrics != null) {
//        val metricsMap = JsonMethods.parse(metrics).extract[Map[String, AnyVal]]
        val peakExecutorMemory =
          metrics.get(PEAK_EXECUTOR_MEMORY).map(_.asInstanceOf[Int]).getOrElse(-1)
        logInfo(s"Get history peak executor memory $peakExecutorMemory")
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
        logInfo(s"Rule ExecutorMemory set executor memory $executorMemory")
        helper.setConf(config.EXECUTOR_MEMORY.key, s"${executorMemory}m")
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
