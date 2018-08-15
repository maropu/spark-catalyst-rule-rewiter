/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.catalyst.ml

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.ml.CatalystConf._
import org.apache.spark.sql.catalyst.plans.logical.{Histogram, LogicalPlan, Project, Statistics}
import org.apache.spark.sql.internal.SQLConf


/**
 * This optimizer rule removes features with low variance; it removes all features whose
 * variance doesn't meet some threshold. You can control this threshold by
 * `spark.sql.optimizer.featureSelection.varianceThreshold` (0.05 by default).
 */
object VarianceThreshold extends MLAwareRuleBase {

  private def hasColumnHistogram(s: Statistics): Boolean = {
    s.attributeStats.exists { case (_, stat) =>
      stat.histogram.isDefined
    }
  }

  private def checkVariance(
      attr: Attribute,
      histgramOption: Option[Histogram],
      varianceThreshold: Double): Boolean = {
    // TODO: Since binary types are not supported in histograms but they could frequently appear
    // in user schemas, we would be better to handle the case here.
    histgramOption.forall { hist =>
      // TODO: Make the value more precise by using `HistogramBin.ndv`
      val dataSeq = hist.bins.map { bin => (bin.hi + bin.lo) / 2 }
      val avg = dataSeq.sum / dataSeq.length
      val variance = dataSeq.map { d => Math.pow(avg - d, 2.0) }.sum / dataSeq.length
      if (varianceThreshold > variance) {
        logWarning(s"Column $attr filtered out because of low variance: $variance")
        false
      } else {
        true
      }
    }
  }

  override def doApply(plan: LogicalPlan): LogicalPlan = plan match {
    case p if SQLConf.get.varianceThresholdEnabled && hasColumnHistogram(p.stats) =>
      val attributeStats = p.stats.attributeStats
      val outputAttrs = p.output
      val threshold = SQLConf.get.varianceThresholdValue
      val projectList = outputAttrs.zip(outputAttrs.map { a => attributeStats.get(a)}).flatMap {
        case (attr, Some(stat)) if !checkVariance(attr, stat.histogram, threshold) => None
        case (attr, _) => Some(attr)
      }
      if (projectList != outputAttrs) {
        println("origOutput:" + outputAttrs)
        println("projectList:" + projectList)
        println("p:" + p)
        Project(projectList, p)
      } else {
        p
      }

    case p => p
  }
}
