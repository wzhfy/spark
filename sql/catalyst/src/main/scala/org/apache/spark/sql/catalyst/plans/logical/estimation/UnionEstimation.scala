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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.cbo.CardinalityEstimator
import org.apache.spark.sql.catalyst.plans.logical.{BasicStat, NumericBasicStat, StringBasicStat, Union}
import org.apache.spark.sql.types._


object UnionEstimation {
  def apply(plan: Union): Unit = {

    val leftSize = plan.left.planStat.totalSize
    val leftRows = plan.left.planStat.rowCount
    val rightSize = plan.right.planStat.totalSize
    val rightRows = plan.right.planStat.rowCount

    if (leftSize * leftRows == 0) {
      plan.planStat.copyFrom(plan.right.planStat)
      return
    }
    if (rightSize * rightRows == 0) {
      plan.planStat.copyFrom(plan.left.planStat)
      return
    }
    plan.planStat.rowCount = leftRows + rightRows

    // basicStats need to be updated
    var needUpdate = Map[AttributeReference, BasicStat]()

    // Union's output is left's output, so we need to get the mapping between two attribute sets
    val leftAttrs = plan.left.output.filter(_.isInstanceOf[AttributeReference]).map(_
      .asInstanceOf[AttributeReference])
    val rightAttrs = plan.right.output.filter(_.isInstanceOf[AttributeReference]).map(_
      .asInstanceOf[AttributeReference])
    val attrMap = leftAttrs.zip(rightAttrs).toMap

    leftAttrs.foreach { attrRef =>
      val rightAttr = attrMap(attrRef)
      if (plan.left.planStat.basicStats.contains(attrRef) &&
        plan.right.planStat.basicStats.contains(rightAttr)) {

        val newStat: BasicStat = attrRef.dataType match {
          case StringType =>
            val leftStat = plan.left.planStat.basicStats(attrRef).asInstanceOf[StringBasicStat]
            val rightStat = plan.right.planStat.basicStats(rightAttr).asInstanceOf[StringBasicStat]
            val newMax = if (leftStat.max > rightStat.max) leftStat.max else rightStat.max
            val newMin = if (leftStat.min < rightStat.min) leftStat.min else rightStat.min
            val newNdv = numberAfterUnion(leftStat.ndv, rightStat.ndv)
            val newNumNull = numberAfterUnion(leftStat.numNull, rightStat.numNull)
            val newMaxLen = math.max(leftStat.maxLength, rightStat.maxLength)
            val newAvgLen = (BigDecimal(leftRows) * leftStat.avgLength + BigDecimal(rightRows) *
              rightStat.avgLength) / (BigDecimal(leftRows) + BigDecimal(rightRows))
            StringBasicStat(newMax, newMin, newNdv, newNumNull, newMaxLen, newAvgLen.toDouble)
          case dataType: DataType if (dataType.isInstanceOf[NumericType]
            || dataType.isInstanceOf[DateType]
            || dataType.isInstanceOf[TimestampType]) =>
            val leftStat = plan.left.planStat.basicStats(attrRef).asInstanceOf[NumericBasicStat]
            val rightStat = plan.right.planStat.basicStats(rightAttr).asInstanceOf[NumericBasicStat]
            val newMax = math.max(leftStat.max, rightStat.max)
            val newMin = math.min(leftStat.min, rightStat.min)
            // TODO: how to compute ndv after union, max or sum?
            val newNdv = numberAfterUnion(leftStat.ndv, rightStat.ndv)
            val newNumNull = numberAfterUnion(leftStat.numNull, rightStat.numNull)
            NumericBasicStat(newMax, newMin, newNdv, newNumNull)
        }
        needUpdate += (attrRef -> newStat, rightAttr -> newStat)
      }
    }
    // Union's output is left's output
    val totalBasicStats = plan.left.planStat.basicStats ++ plan.right.planStat.basicStats ++
      needUpdate
    val totalHistograms = plan.left.planStat.histograms ++ plan.right.planStat.histograms
    // TODO: update histograms: difficult to form new equi-height histograms
    CardinalityEstimator.updateStatsByOutput(plan, totalBasicStats, totalHistograms)
  }

  def numberAfterUnion(num1: Long, num2: Long): Long = {
    val value1: Double = math.max(num1, num2)
    val value2: Double = num1 + num2
    ((value1 + value2) / 2.0).toLong
  }
}
