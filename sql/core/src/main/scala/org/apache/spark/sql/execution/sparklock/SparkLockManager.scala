/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.sparklock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.lockmgr.{HiveTxnManager, TxnManagerFactory}
import org.apache.hadoop.hive.ql.{Context, QueryPlan}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.sparklock.SparkLockUtils.buildHiveConf
import org.apache.spark.sql.internal.{SQLConf, SessionState}

class SparkLockManager {

}

object SparkLockManager extends Logging {
  def apply(conf: SQLConf): SparkLockManager = new SparkLockManager

  def lock(context: SparkLockContext): Unit = {
    val manager = context.manager
    val hivePlan = context.hivePlan
    val hiveLockContext = context.hiveLockContext
    val sparkUser = context.sparkSession.sparkContext.sparkUser
    logInfo(s"Start lock for output: ${hivePlan.getOutputs} and input: ${hivePlan.getInputs}")
    manager.acquireLocks(hivePlan, hiveLockContext, sparkUser)
  }

  def unlock(qe: QueryExecution): Unit = {
    if (null == qe) {
      return
    }
    val context = qe.sparkLockContext
    val locks = context.hiveLockContext.getHiveLocks
    val manager = context.manager
    if (locks != null && locks.size() > 0) {
      logInfo(s"Start unlock $locks")
      manager.releaseLocks(locks)
    }
  }
}


class SparkLockContext(val qe: QueryExecution) extends Logging {
  lazy val manager: HiveTxnManager = {
    assert(SparkSession.getActiveSession.isDefined)
    val hiveConf = buildHiveConf(SparkSession.getActiveSession.get.sparkContext.conf)
    TxnManagerFactory.getTxnManagerFactory.getTxnManager(hiveConf)
  }
  val sparkSession: SparkSession = qe.sparkSession
  val sessionState: SessionState = sparkSession.sessionState
  val hadoopConf: Configuration = sessionState.newHadoopConf()
  lazy val hiveConf: HiveConf = {
    val sparkConf = sparkSession.sparkContext.conf
    buildHiveConf(sparkConf)
  }
  val hiveLockContext: Context = new Context(hiveConf)
  var hivePlan: QueryPlan = _
}

object SparkLockContext {
  def apply(qe: QueryExecution): SparkLockContext = new SparkLockContext(qe)
}