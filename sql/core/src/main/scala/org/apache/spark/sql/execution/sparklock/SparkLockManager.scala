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
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager
import org.apache.hadoop.hive.ql.{Context, QueryPlan}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.{SQLConf, SessionState}

class SparkLockManager {

}

object SparkLockManager {
  var manager: DbTxnManager = _

  def apply(conf: SQLConf): SparkLockManager = new SparkLockManager

  def lock(context: SparkLockContext): Unit = {
    manager.acquireLocks(context.hivePlan, context.hiveLockContext, context.sparkSession.sparkContext.sparkUser)
    // 1. 构建 LockRequest
    // 2. hms.lock() => lockId
    // 3. 缓存qe和lockId的对应关系
    // 4. 启动心跳汇报进程
    // 5. 缓存qe和心跳会把进程的对应关系
  }

  def unlock(qe: QueryExecution): Unit = {
    manager.releaseLocks(qe.sparkLockContext.hiveLockContext.getHiveLocks)
  }
}


class SparkLockContext(val qe: QueryExecution) extends Logging {
  val sparkSession: SparkSession = qe.sparkSession
  val sessionState: SessionState = sparkSession.sessionState
  var logicalPlan: Option[LogicalPlan] = Option.empty
  var sparkPlan: Option[SparkPlan] = Option.empty
//  val lockId: Option[Long] = Option.empty
//  val inputs: Seq[ReadEntity] = Seq.empty
//  val outputs: Seq[WriteEntity] = Seq.empty
  val hadoopConf: Configuration = sessionState.newHadoopConf()
  val hiveLockContext: Context = new Context(hadoopConf)
  var hivePlan: QueryPlan = _
}

object SparkLockContext {
  def apply(qe: QueryExecution): SparkLockContext = new SparkLockContext(qe)
}