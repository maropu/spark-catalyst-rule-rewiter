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

package org.apache.spark.sql.catalyst.ml

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, PredicateHelper}
import org.apache.spark.sql.catalyst.ml.CatalystConf._
import org.apache.spark.sql.catalyst.ml.copied.StarSchemaDetection._
import org.apache.spark.sql.catalyst.optimizer.ReorderJoin
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Sample}
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes down [[Sample]] beneath the inputs of inner/outer joins under some conditions.
 */
private[ml] object SamplePushDown extends MLAwareRuleBase with PredicateHelper {

  def doApply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case sample @ Sample(_, _, _, _, child) => child match {
      // TODO: Cover outer joins here
      case ExtractFiltersAndInnerJoins(input, conditions)
          if SQLConf.get.samplePushDownEnabled && conditions.nonEmpty =>
        if (input.size == 2) {
          require(conditions.size == 1)

          val Seq((t1, jt1), (t2, jt2)) = input
          def hasUniqueColumn(plan: LogicalPlan, c: Attribute): Boolean = {
            plan.outputSet.contains(c) && isUnique(c, plan)
          }
          conditions.head match {
            case EqualTo(l: Attribute, r: Attribute)
                if hasUniqueColumn(t2, r) || hasUniqueColumn(t2, l) =>
              Join(sample.copy(child = t1), t2, jt1, Some(conditions.head))
            case EqualTo(l: Attribute, r: Attribute)
                if hasUniqueColumn(t1, l) || hasUniqueColumn(t1, r) =>
              Join(t1, sample.copy(child = t2), jt1, Some(conditions.head))
            case _ =>
              sample
          }
        } else {
          // Finds the eligible star plans. Currently, it only returns
          // the star join with the largest fact table.
          val eligibleJoins = input.collect{ case (plan, Inner) => plan }
          val starJoinPlan = findStarJoins(eligibleJoins, conditions)
          if (starJoinPlan.nonEmpty) {
            val (factTable, dimTables) = (starJoinPlan.head, starJoinPlan.tail)
            val otherTables = input.filterNot { case (p, _) => starJoinPlan.contains(p) }
            val factTableTableRefs = factTable.outputSet
            val (factTableConditions, otherConditions) = conditions.partition(
              e => e.references.subsetOf(factTableTableRefs) && canEvaluateWithinJoin(e))
            val newFactTable = if (factTableConditions.nonEmpty) {
              Filter(factTableConditions.reduceLeft(And), factTable)
            } else {
              factTable
            }
            val sampledFactTable = sample.copy(child = newFactTable)
            val newStarJoinPlan = (sampledFactTable +: dimTables).map(plan => (plan, Inner))
            ReorderJoin.createOrderedJoin(newStarJoinPlan ++ otherTables, otherConditions)
          } else {
            sample
          }
        }
      case _ =>
        sample
    }
  }
}
