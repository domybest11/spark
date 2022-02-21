package org.apache.spark.sql.execution.sparklock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}

case class SparkLockRule(context: SparkLockRuleContext) extends Rule[SparkPlan] {

  override def apply(sparkPlan: SparkPlan): SparkPlan = applyInternal(sparkPlan)

  def applyInternal(sparkPlan: SparkPlan): SparkPlan = {
    val sparkLockContext = context.sparkLockContext

    // 1. parse logic plan, find which table has tags (should lock)
    //    val shouldLockTable: ArrayBuffer[TableIdentifier] =
    //      parseLogicalPlan(logicalPlan.get, context).distinct

    sparkLockContext.logicalPlan = sparkPlan.logicalLink
    sparkLockContext.sparkPlan = Option(sparkPlan)

    // 2. parse spark plan, find input and output
    val hivePlan = HivePlanFinder.buildHivePlan(sparkPlan, sparkLockContext)
    sparkLockContext.hivePlan = hivePlan

    // 3. SparkLockManager#lock
    SparkLockManager.lock(sparkLockContext)

    sparkPlan
  }
}



case class SparkLockRuleContext(qe: QueryExecution) {
  val session: SparkSession = qe.sparkSession
  val sparkLockContext: SparkLockContext = new SparkLockContext(qe)
}