
package org.apache.spark.deploy

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.HttpClientUtils

class SparkConfHelper(sparkConf: SparkConf) extends Logging {

  private val EFFECTIVE_RULES = "spark.deploy.autoConfEffectiveRules"

  private val conf = new mutable.LinkedHashMap[String, String]()
  private val effectiveRules = new mutable.HashSet[String]()

  private lazy val jobTag = sparkConf.getOption("spark.deploy.jobTag")
  private lazy val metrics = HttpClientUtils.getInstance().getJobHistoryMetric(jobTag.get)

  private val rules = Seq(
    ExecutorMemoryRule(sparkConf),
    AllocationRatioRule(sparkConf))

  def execute: Unit = {
    rules.foreach(r => r.apply(this))
  }

  def addEffectiveRules(rule: String): Unit = {
    effectiveRules.add(rule)
  }

  def setConf(key: String, value: String): Unit = {
    conf.put(key, value)
  }

  def getMetricByKey(key: String): Option[AnyRef] = {
    if (metrics == null) {
      return null
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