package org.apache.spark.sql.execution.sparklock

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SparkUnlockListener extends QueryExecutionListener with Logging {
  /**
   * A callback function that will be called when a query executed successfully.
   *
   * @param funcName   name of the action that triggered this query.
   * @param qe         the QueryExecution object that carries detail information like logical plan,
   *                   physical plan, etc.
   * @param durationNs the execution time for this query in nanoseconds.
   * @note This can be invoked by multiple different threads.
   */
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logInfo(s"query executed success, and unlock the lock " +
      qe.sparkLockContext.hiveLockContext.getHiveLocks)
    SparkLockManager.unlock(qe)
  }

  /**
   * A callback function that will be called when a query execution failed.
   *
   * @param funcName  the name of the action that triggered this query.
   * @param qe        the QueryExecution object that carries detail information like logical plan,
   *                  physical plan, etc.
   * @param exception the exception that failed this query. If `java.lang.Error` is thrown during
   *                  execution, it will be wrapped with an `Exception` and it can be accessed by
   *                  `exception.getCause`.
   * @note This can be invoked by multiple different threads.
   */
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError(s"query executed success, and unlock the lock " +
      qe.sparkLockContext.hiveLockContext.getHiveLocks, exception)
    SparkLockManager.unlock(qe)
  }
}
