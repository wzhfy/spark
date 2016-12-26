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

package org.apache.spark.sql.catalyst.plans.logical.estimation


object AggregateEstimation {
  def apply(plan: Aggregate): Unit = {

    if (plan.child.planStat.rowCount == 0) {
      return
    }
    // Note: there's a special case here when child.planStat.rowCount == 0 && child.planStat
    // .totalSize != 0, it's probably due to a project with empty projectList. In this case, we
    // still need to do estimation.
    var outputRows: BigInt = 1
    plan.groupingExpressions.foreach {
      case attrRef: AttributeReference if plan.child.planStat.basicStats.contains(attrRef) =>
        val ndv = plan.child.planStat.basicStats(attrRef).ndv
        val rowsPerDv = BigDecimal(plan.child.planStat.rowCount) / ndv
        if (EstimationUtils.isPrimaryKey(rowsPerDv)) {
          // if the column is unique, do not multiply ndv of non-unique columns
          updateStats(ndv, plan)
          return
        } else {
          outputRows *= ndv
        }
      case _ =>
        // It's not a column (e.g., udf), or we don't have its stats, just don't estimate
        plan.planStat.copyFrom(plan.child.planStat)
        return
    }
    updateStats(outputRows, plan)
  }

  private def updateStats(outputRows: BigInt, plan: Aggregate): Unit = {
    plan.planStat.rowCount = if (outputRows < plan.child.planStat.rowCount) {
      outputRows
    } else {
      plan.child.planStat.rowCount
    }
    // maybe we can update basicStats according to aggregateExpressions
    CardinalityEstimator.updateStatsByOutput(plan, plan.child.planStat.basicStats, plan.child
      .planStat.histograms)
  }
}
