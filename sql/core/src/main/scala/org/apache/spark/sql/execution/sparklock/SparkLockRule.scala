package org.apache.spark.sql.execution.sparklock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_LOCK_ENABLE

case class SparkLockRule(context: SparkLockRuleContext) extends Rule[SparkPlan] {

  override def apply(sparkPlan: SparkPlan): SparkPlan = {
    if (SparkSession.active.sparkContext.conf.get(SPARK_LOCK_ENABLE)) {
      try {
        applyInternal(sparkPlan)
      } catch {
        case e: Throwable =>
          logError("failed to exec spark lock", e)
      }
    }
    sparkPlan
  }

  def applyInternal(sparkPlan: SparkPlan): Unit = {
    val sparkLockContext = context.sparkLockContext

    // 2. parse spark plan, find input and output
    val hivePlan = HivePlanFinder.buildHivePlan(sparkPlan, sparkLockContext)
    sparkLockContext.hivePlan = hivePlan

    // 3. SparkLockManager#lock
    SparkLockManager.lock(sparkLockContext)
  }
}



case class SparkLockRuleContext(qe: QueryExecution) {
  val session: SparkSession = qe.sparkSession
  val sparkLockContext: SparkLockContext = qe.sparkLockContext
}