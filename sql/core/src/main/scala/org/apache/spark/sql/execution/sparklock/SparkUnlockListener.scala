package org.apache.spark.sql.execution.sparklock

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

class SparkUnlockListener extends SparkListener with Logging {

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case _ =>
  }

  def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = try {
      SparkLockManager.unlock(event.qe)
    } catch {
      case t: Throwable =>
        logError("unlock encounter an error", t)
    }
}
