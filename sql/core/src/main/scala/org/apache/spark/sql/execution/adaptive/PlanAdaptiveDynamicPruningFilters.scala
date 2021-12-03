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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, BindReferences, DynamicPruningExpression, DynamicPruningSubquery, Expression, ExprId, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}

/**
 * A rule to insert dynamic pruning predicates in order to reuse the results of broadcast/exchange.
 */
case class PlanAdaptiveDynamicPruningFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPruningEnabled) {
      return plan
    }

    plan transformAllExpressions {
      case DynamicPruningExpression(InSubqueryExec(
        value, SubqueryAdaptiveBroadcastExec(name, index, onlyInBroadcast, buildPlan, buildKeys,
          adaptivePlan: AdaptiveSparkPlanExec), exprId, _)) =>
        planDynamicPartitionPruning(
          value, name, index, onlyInBroadcast, buildPlan, buildKeys, adaptivePlan, exprId)

      case DynamicPruningExpression(i @ InBloomFilterSubqueryExec(
        _, SubqueryAdaptiveShuffleExec(name, buildPlan, _,
          adaptivePlan: AdaptiveSparkPlanExec), _, _)) =>
        // Used to resolve the nested DPP is inside the InBloomFilterSubquery.
        val subqueryMap = mutable.HashMap.empty[Long, DynamicPruningExpression]
        adaptivePlan.inputPlan.collectLeaves().foreach { f =>
          f.expressions.foreach {
            case DynamicPruningExpression(InSubqueryExec(
            value, SubqueryAdaptiveBroadcastExec(name, index, onlyInBroadcast, buildPlan, buildKeys,
            adaptivePlan: AdaptiveSparkPlanExec), exprId, _)) =>
              subqueryMap.put(exprId.id,
                planDynamicPartitionPruning(
                  value, name, index, onlyInBroadcast, buildPlan, buildKeys, adaptivePlan, exprId))
            case _ =>
          }
        }

        val session = adaptivePlan.context.session
        val planner = session.sessionState.planner
        val sparkPlan = QueryExecution.createSparkPlan(session, planner, buildPlan)
          .transformAllExpressions {
            case d: DynamicPruningSubquery => subqueryMap.getOrElse(d.exprId.id, d)
          }

        val newAdaptivePlan = adaptivePlan.copy(inputPlan = sparkPlan)
        DynamicPruningExpression(i.copy(plan = SubqueryExec(name, newAdaptivePlan)))
    }
  }

  private def planDynamicPartitionPruning(
      value: Expression,
      name: String,
      index: Int,
      onlyInBroadcast: Boolean,
      buildPlan: LogicalPlan,
      buildKeys: Seq[Expression],
      adaptivePlan: AdaptiveSparkPlanExec,
      exprId: ExprId): DynamicPruningExpression = {
    val packedKeys = BindReferences.bindReferences(
      HashJoin.rewriteKeyExpr(buildKeys), adaptivePlan.executedPlan.output)
    val mode = HashedRelationBroadcastMode(packedKeys)
    // plan a broadcast exchange of the build side of the join
    val exchange = BroadcastExchangeExec(mode, adaptivePlan.executedPlan)

    val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
      find(rootPlan) {
        case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
          left.sameResult(exchange)
        case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
          right.sameResult(exchange)
        case _ => false
      }.isDefined

    if (canReuseExchange) {
      exchange.setLogicalLink(adaptivePlan.executedPlan.logicalLink.get)
      val newAdaptivePlan = adaptivePlan.copy(inputPlan = exchange)

      val broadcastValues = SubqueryBroadcastExec(
        name, index, buildKeys, newAdaptivePlan)
      DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
    } else if (onlyInBroadcast) {
      DynamicPruningExpression(Literal.TrueLiteral)
    } else {
      // we need to apply an aggregate on the buildPlan in order to be column pruned
      val alias = Alias(buildKeys(index), buildKeys(index).toString)()
      val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)

      val session = adaptivePlan.context.session
      val planner = session.sessionState.planner
      // Here we can't call the QueryExecution.prepareExecutedPlan() method to
      // get the sparkPlan as Non-AQE use case, which will cause the physical
      // plan optimization rules be inserted twice, once in AQE framework and
      // another in prepareExecutedPlan() method.
      val sparkPlan = QueryExecution.createSparkPlan(session, planner, aggregate)
      val newAdaptivePlan = adaptivePlan.copy(inputPlan = sparkPlan)
      val values = SubqueryExec(name, newAdaptivePlan)
      DynamicPruningExpression(InSubqueryExec(value, values, exprId))
    }
  }
}
