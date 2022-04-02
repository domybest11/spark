package org.apache.spark.sql.execution.sparklock

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_LOCK_ENABLE

class SparkUnlockListener extends SparkListener with Logging {

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case _ =>
  }

  def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit =
    if (SparkSession.active.sparkContext.conf.get(SPARK_LOCK_ENABLE)) {
      try {
        SparkLockManager.unlock(event.qe)
      } catch {
        case t: Throwable =>
          logError("unlock encounter an error", t)
      }
    }
}
