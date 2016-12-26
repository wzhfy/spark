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

import scala.math.BigDecimal.RoundingMode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.types.StringType


object EstimationUtils extends Logging {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreachUp { node =>
      if (shouldEstimate(node)) {
        node match {
          case f: Filter =>
            FilterCardEstimator(f)
          case j: Join =>
            JoinCardEstimator(j)
          case a: Aggregate =>
            AggregateCardEstimator(a)
          case u: Union =>
            UnionCardEstimator(u)
          case p: Project =>
            p.planStat.rowCount = p.child.planStat.rowCount
            updateStatsByOutput(p, p.child.planStat.basicStats, p.child.planStat.histograms)
          case l: Limit =>
            val limit = l.limitExpr.eval().asInstanceOf[Int]
            if (limit >= 0) {
              val childRowCount = l.child.planStat.rowCount
              val childSize = l.child.planStat.totalSize
              if (childRowCount <= limit) {
                l.planStat.rowCount = childRowCount
                l.planStat.totalSize = childSize
              } else {
                l.planStat.rowCount = limit
                l.planStat.totalSize = RoundingToBigInt(BigDecimal(limit) * BigDecimal(childSize)
                  / BigDecimal(childRowCount))
              }
              // we do not update column stats because the limit operation has randomness
              l.planStat.basicStats = Map()
              l.planStat.histograms = Map()
              l.planStat.equiHeightHgmMins = Map()
              l.planStat.bucketHeights = Map()
            }
          case s: Sort =>
            // no influence on stats
            s.planStat.copyFrom(s.child.planStat)
          case Intersect(_, _) | Except(_, _) =>
            node.planStat.copyFrom(node.children.head.planStat)
          case lp: LogicalPlan =>
            logDebug("[CBO] Unsupported LogicalPlan: " + lp.nodeName)
          //            defaultEstimation(lp)
        }
      }
      node.planStat.estimated = true
    }
  }

  /**
   * 1. Calculate the new totalSize for this plan.
   * 2. Collect necessary column stats for this plan based on its output.
   */
  def updateStatsByOutput(
                           plan: LogicalPlan,
                           totalBasicStats: Map[AttributeReference, BasicStat],
                           totalHistograms: Map[AttributeReference, Array[Bucket]]): Unit = {

    // 1. deal with alias
    var aliasToColMap = Map[AttributeReference, AttributeReference]()
    plan.expressions.foreach {
      case alias @ Alias(child, name) =>
        child match {
          case ar: AttributeReference =>
            aliasToColMap += (alias.toAttribute.asInstanceOf[AttributeReference] -> ar)
          case _ =>
        }
      case _ =>
    }

    // 2. Calculate size of a single row. The difficulty is how to get the size of StringType.
    var singleRowSize: Double = 0
    plan.output.foreach {
      case ar: AttributeReference if ar.dataType == StringType && totalBasicStats
        .contains(aliasToColMap.getOrElse(ar, ar)) =>
        // Spark sql use UTF8String. UTF-8 encoding uses only one byte for ASCII chars and two or
        // more bytes for other unicode symbols (e.g. Chinese characters). Now we only care about
        // ASCII chars, so the string length is the number of bytes used for that string.
        singleRowSize += totalBasicStats(aliasToColMap.getOrElse(ar, ar))
          .asInstanceOf[StringBasicStat].avgLength
      case ar: AttributeReference if ar.dataType.isInstanceOf[DecimalType] && DecimalType
        .isFixed(ar.dataType) =>
        if (ar.dataType.asInstanceOf[DecimalType].precision <= 18) {
          singleRowSize += 8
        } else {
          singleRowSize += 16
        }
      case a: Attribute =>
        singleRowSize += a.dataType.defaultSize
    }
    plan.planStat.totalSize = RoundingToBigInt(BigDecimal(plan.planStat.rowCount) * singleRowSize)

    // 3. Collect necessary column stats.
    var totalHgmMins = Map[AttributeReference, Double]()
    var totalHeights = Map[AttributeReference, BigDecimal]()
    plan.children.foreach { child =>
      totalHgmMins ++= child.planStat.equiHeightHgmMins
      totalHeights ++= child.planStat.bucketHeights
    }
    plan.output.foreach {
      case curAR: AttributeReference =>
        val originAR = aliasToColMap.getOrElse(curAR, curAR)
        if (totalBasicStats.contains(originAR)) {
          plan.planStat.basicStats += (curAR -> totalBasicStats(originAR))
        }
        if (totalHistograms.contains(originAR)) {
          plan.planStat.histograms += (curAR -> totalHistograms(originAR))
        }
        if (totalHgmMins.contains(originAR)) {
          plan.planStat.equiHeightHgmMins += (curAR -> totalHgmMins(originAR))
        }
        if (totalHeights.contains(originAR)) {
          plan.planStat.bucketHeights += (curAR -> totalHeights(originAR))
        }
      case _ =>
    }
  }

  /**
   * update columns' ndv's so that they won't be larger than rowCount
   */
  def updateNdvBasedOnRows(planStat: PlanStatistics): Unit = {
    planStat.basicStats.keySet.foreach { col =>
      val stat = planStat.basicStats(col)
      if (stat.ndv > planStat.rowCount) stat.ndv = planStat.rowCount.toLong
    }
  }

  def rowCountsExist(plans: LogicalPlan*): Boolean =
    plans.forall(_.statistics.rowCount.isDefined)

  def columnStatsExist(statsAndAttr: (Statistics, AttributeReference)*): Boolean = {
    statsAndAttr.forall { case (stats, attr) =>
      stats.attributeStats.contains(attr)
    }
  }

  def ceil(bigDecimal: BigDecimal): BigInt = bigDecimal.setScale(0, RoundingMode.CEILING).toBigInt()

  def getRowSize(attributes: Seq[Attribute], colStats: AttributeMap[ColumnStat]): Long = {
    attributes.map { attr =>
      if (colStats.contains(attr)) {
        attr.dataType match {
          case StringType =>
            // base + offset + numBytes
            colStats(attr).avgLen + 8 + 4
          case _ =>
            colStats(attr).avgLen
        }
      } else {
        attr.dataType.defaultSize
      }
    }.sum
  }
}

/** Attribute Reference extractor */
object ExtractAttr {
  def unapply(exp: Expression): Option[AttributeReference] = exp match {
    case ar: AttributeReference => Some(ar)
    case _ => None
  }
}
