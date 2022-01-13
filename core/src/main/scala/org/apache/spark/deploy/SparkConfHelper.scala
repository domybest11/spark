
package org.apache.spark.deploy

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.{HttpClientUtils, Utils}

class SparkConfHelper(
    sparkConf: SparkConf,
    httpClient: HttpClientUtils = HttpClientUtils.getInstance())
  extends Logging {

  private val EFFECTIVE_RULES = "spark.deploy.autoConfEffectiveRules"

  private val conf = new mutable.LinkedHashMap[String, String]()
  private val effectiveRules = new mutable.HashSet[String]()

  private lazy val jobTag = sparkConf.getOption("spark.deploy.jobTag")
  private lazy val metrics = httpClient.getJobHistoryMetric(jobTag.get)

  private val rules = Seq(
    ExecutorMemoryRule(sparkConf),
    DataSourceGrayScaleRelease(sparkConf),
    AllocationRatioRule(sparkConf),
    RepartitionBeforeWriteTableRule(sparkConf),
    PushShuffleRule(sparkConf)
  )

  def execute: Unit = {
    val excludeRules = sparkConf.getOption(config.HBO_EXCLUDE_RULES.key)
      .map(Utils.stringToSeq).getOrElse(Seq.empty)
    rules.foreach(rule =>
      if (excludeRules.contains(rule.ruleName)) {
        logInfo(s"HBO rule '${rule.ruleName}' is excluded from the confHelper.")
      } else {
        rule.apply(this)
      })
  }

  def addEffectiveRules(rule: String): Unit = {
    effectiveRules.add(rule)
  }

  def setConf(key: String, value: String): Unit = {
    conf.put(key, value)
  }

  def getMetricByKey(key: String): Option[AnyRef] = {
    if (metrics == null) {
      return None
    }
    Option(metrics.get(key))
  }

  def getJobTag(): Option[String] = {
    jobTag
  }

  def applySparkConf: Unit = {
    execute
    conf.foreach(entrySet => sparkConf.set(entrySet._1, entrySet._2))
    if (effectiveRules.nonEmpty) {
      sparkConf.set(EFFECTIVE_RULES, effectiveRules.mkString(","))
    }
  }
}