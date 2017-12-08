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

package org.apache.spark.sql.catalyst.statsEstimation

import java.sql.{Date, Timestamp}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, EqualTo, Expression, GreaterThanOrEqual, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DateType, TimestampType, _}


class JoinEstimationSuite extends StatsEstimationTestBase {

  /** Set up tables and its columns for testing */
  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("key-1-5") -> ColumnStat(distinctCount = 5, min = Some(1), max = Some(5), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key-5-9") -> ColumnStat(distinctCount = 5, min = Some(5), max = Some(9), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key-1-2") -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key-2-4") -> ColumnStat(distinctCount = 3, min = Some(2), max = Some(4), nullCount = 0,
      avgLen = 4, maxLen = 4),
    attr("key-2-3") -> ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  // Suppose table1 (key-1-5 int, key-5-9 int) has 5 records: (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
  private val table1 = StatsTestPlan(
    outputList = Seq("key-1-5", "key-5-9").map(nameToAttr),
    rowCount = 5,
    attributeStats = AttributeMap(Seq("key-1-5", "key-5-9").map(nameToColInfo)))

  // Suppose table2 (key-1-2 int, key-2-4 int) has 3 records: (1, 2), (2, 3), (2, 4)
  private val table2 = StatsTestPlan(
    outputList = Seq("key-1-2", "key-2-4").map(nameToAttr),
    rowCount = 3,
    attributeStats = AttributeMap(Seq("key-1-2", "key-2-4").map(nameToColInfo)))

  // Suppose table3 (key-1-2 int, key-2-3 int) has 2 records: (1, 2), (2, 3)
  private val table3 = StatsTestPlan(
    outputList = Seq("key-1-2", "key-2-3").map(nameToAttr),
    rowCount = 2,
    attributeStats = AttributeMap(Seq("key-1-2", "key-2-3").map(nameToColInfo)))

  private def estimateByHistogram(
      histogram1: Histogram,
      histogram2: Histogram,
      expectedMin: Double,
      expectedMax: Double,
      expectedNdv: Long,
      expectedRows: Long): Unit = {
    val col1 = attr("key1")
    val col2 = attr("key2")
    val c1 = generateJoinChild(col1, histogram1, expectedMin, expectedMax)
    val c2 = generateJoinChild(col2, histogram2, expectedMin, expectedMax)

    val c1JoinC2 = Join(c1, c2, Inner, Some(EqualTo(col1, col2)))
    val c2JoinC1 = Join(c2, c1, Inner, Some(EqualTo(col2, col1)))
    val expectedStatsAfterJoin = Statistics(
      sizeInBytes = expectedRows * (8 + 2 * 4),
      rowCount = Some(expectedRows),
      attributeStats = AttributeMap(Seq(
        col1 -> c1.stats.attributeStats(col1).copy(
          distinctCount = expectedNdv, min = Some(expectedMin), max = Some(expectedMax)),
        col2 -> c2.stats.attributeStats(col2).copy(
          distinctCount = expectedNdv, min = Some(expectedMin), max = Some(expectedMax))))
    )

    // Join order should not affect estimation result.
    Seq(c1JoinC2, c2JoinC1).foreach { join =>
      assert(join.stats == expectedStatsAfterJoin)
    }
  }

  private def generateJoinChild(
      col: Attribute,
      histogram: Histogram,
      expectedMin: Double,
      expectedMax: Double): LogicalPlan = {
    val colStat = inferColumnStat(histogram)
    val t = StatsTestPlan(
      outputList = Seq(col),
      rowCount = (histogram.height * histogram.bins.length).toLong,
      attributeStats = AttributeMap(Seq(col -> colStat)))

    val filterCondition = new ArrayBuffer[Expression]()
    if (expectedMin > colStat.min.get.toString.toDouble) {
      filterCondition += GreaterThanOrEqual(col, Literal(expectedMin))
    }
    if (expectedMax < colStat.max.get.toString.toDouble) {
      filterCondition += LessThanOrEqual(col, Literal(expectedMax))
    }
    if (filterCondition.isEmpty) t else Filter(filterCondition.reduce(And), t)
  }

  /** Column statistics should be consistent with histograms in tests. */
  private def inferColumnStat(histogram: Histogram): ColumnStat = {
    var ndv = 0L
    for (i <- histogram.bins.indices) {
      val bin = histogram.bins(i)
      if (i == 0 || bin.hi != histogram.bins(i - 1).hi) {
        ndv += bin.ndv
      }
    }
    ColumnStat(distinctCount = ndv, min = Some(histogram.bins.head.lo),
      max = Some(histogram.bins.last.hi), nullCount = 0, avgLen = 4, maxLen = 4,
      histogram = Some(histogram))
  }

  test("equi-height histograms: a bin is contained by another one") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 10, hi = 30, ndv = 10), HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 100, ndv = 40)))
    // test bin trimming
    val (t1, h1) = trimBin(histogram2.bins(0), height = 100, min = 10, max = 60)
    assert(t1 == HistogramBin(lo = 10, hi = 50, ndv = 40) && h1 == 80)
    val (t2, h2) = trimBin(histogram2.bins(1), height = 100, min = 10, max = 60)
    assert(t2 == HistogramBin(lo = 50, hi = 60, ndv = 8) && h2 == 20)

    val expectedRanges = Seq(
      OverlappedRange(10, 30, 10, 40*1/2, 300, 80*1/2),
      OverlappedRange(30, 50, 30*2/3, 40*1/2, 300*2/3, 80*1/2),
      OverlappedRange(50, 60, 30*1/3, 8, 300*1/3, 20)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, newMin = 10D, newMax = 60D)))

    estimateByHistogram(
      histogram1 = histogram1,
      histogram2 = histogram2,
      expectedMin = 10D,
      expectedMax = 60D,
      // 10 + 20 + 8
      expectedNdv = 38L,
      // 300*40/20 + 200*40/20 + 100*20/10
      expectedRows = 1200L)
  }

  test("equi-height histograms: skew distribution (some bins have only one value)") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 30, hi = 30, ndv = 1),
      HistogramBin(lo = 30, hi = 30, ndv = 1),
      HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 100, ndv = 40)))
    // test bin trimming
    val (t1, h1) = trimBin(histogram2.bins(0), height = 100, min = 30, max = 60)
    assert(t1 == HistogramBin(lo = 30, hi = 50, ndv = 20) && h1 == 40)
    val (t2, h2) = trimBin(histogram2.bins(1), height = 100, min = 30, max = 60)
    assert(t2 ==HistogramBin(lo = 50, hi = 60, ndv = 8) && h2 == 20)

    val expectedRanges = Seq(
      OverlappedRange(30, 30, 1, 1, 300, 40/20),
      OverlappedRange(30, 30, 1, 1, 300, 40/20),
      OverlappedRange(30, 50, 30*2/3, 20, 300*2/3, 40),
      OverlappedRange(50, 60, 30*1/3, 8, 300*1/3, 20)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, newMin = 30D, newMax = 60D)))

    estimateByHistogram(
      histogram1 = histogram1,
      histogram2 = histogram2,
      expectedMin = 30D,
      expectedMax = 60D,
      // 1 + 20 + 8
      expectedNdv = 29L,
      // 300*2/1 + 300*2/1 + 200*40/20 + 100*20/10
      expectedRows = 1800L)
  }

  test("equi-height histograms: skew distribution (histograms have different skewed values") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 30, hi = 30, ndv = 1), HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 50, ndv = 1)))
    // test bin trimming
    val (t1, h1) = trimBin(histogram2.bins(0), height = 100, min = 30, max = 50)
    assert(t1 == HistogramBin(lo = 30, hi = 50, ndv = 20) && h1 == 40)
    val (t2, h2) = trimBin(histogram1.bins(1), height = 300, min = 30, max = 50)
    assert(t2 == HistogramBin(lo = 30, hi = 50, ndv = 20) && h2 == 200)

    val expectedRanges = Seq(
      OverlappedRange(30, 30, 1, 1, 300, 40/20),
      OverlappedRange(30, 50, 20, 20, 200, 40),
      OverlappedRange(50, 50, 1, 1, 200/20, 100)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, newMin = 30D, newMax = 50D)))

    estimateByHistogram(
      histogram1 = histogram1,
      histogram2 = histogram2,
      expectedMin = 30D,
      expectedMax = 50D,
      // 1 + 20
      expectedNdv = 21L,
      // 300*2/1 + 200*40/20 + 10*100/1
      expectedRows = 2000L)
  }

  test("equi-height histograms: skew distribution (both histograms have the same skewed value") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 30, hi = 30, ndv = 1), HistogramBin(lo = 30, hi = 60, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 30, ndv = 30), HistogramBin(lo = 30, hi = 30, ndv = 1)))
    // test bin trimming
    val (t1, h1) = trimBin(histogram2.bins(0), height = 100, min = 30, max = 60)
    assert(t1 == HistogramBin(lo = 30, hi = 50, ndv = 20) && h1 == 40)
    val (t2, h2) = trimBin(histogram2.bins(1), height = 100, min = 30, max = 60)
    assert(t2 ==HistogramBin(lo = 50, hi = 60, ndv = 8) && h2 == 20)

    val expectedRanges = Seq(
      OverlappedRange(30, 30, 1, 1, 300, 40/20),
      OverlappedRange(30, 50, 30*2/3, 20, 300*2/3, 40),
      OverlappedRange(50, 60, 30*1/3, 8, 300*1/3, 20)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, newMin = 30D, newMax = 60D)))

    estimateByHistogram(
      histogram1 = histogram1,
      histogram2 = histogram2,
      expectedMin = 30D,
      expectedMax = 60D,
      // 1 + 20 + 8
      expectedNdv = 29L,
      // 300*20/1 + 200*40/20 + 100*20/10
      expectedRows = 1200L)
  }

  test("equi-height histograms: a bin has only one value after trimming") {
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 50, hi = 60, ndv = 10), HistogramBin(lo = 60, hi = 75, ndv = 3)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = 0, hi = 50, ndv = 50), HistogramBin(lo = 50, hi = 100, ndv = 40)))
    // test bin trimming
    val (t1, h1) = trimBin(histogram2.bins(0), height = 100, min = 50, max = 75)
    assert(t1 == HistogramBin(lo = 50, hi = 50, ndv = 1) && h1 == 2)
    val (t2, h2) = trimBin(histogram2.bins(1), height = 100, min = 50, max = 75)
    assert(t2 == HistogramBin(lo = 50, hi = 75, ndv = 20) && h2 == 50)

    val expectedRanges = Seq(
      OverlappedRange(50, 50, 1, 1, 300/10, 2),
      OverlappedRange(50, 60, 10, 20*10/25, 300, 50*10/25),
      OverlappedRange(60, 75, 3, 20*15/25, 300, 50*15/25)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, newMin = 50D, newMax = 75D)))

    estimateByHistogram(
      histogram1 = histogram1,
      histogram2 = histogram2,
      expectedMin = 50D,
      expectedMax = 75D,
      // 1 + 8 + 3
      expectedNdv = 12L,
      // 30*2/1 + 300*20/10 + 300*30/12
      expectedRows = 1410L)
  }

  test("equi-height histograms: skip and trim bins by min/max") {
    // This case can happen when estimating join after a filter.
    val histogram1 = Histogram(height = 300, Array(
      HistogramBin(lo = 1, hi = 30, ndv = 10),
      HistogramBin(lo = 30, hi = 60, ndv = 30),
      HistogramBin(lo = 60, hi = 90, ndv = 6),
      HistogramBin(lo = 90, hi = 200, ndv = 30)))
    val histogram2 = Histogram(height = 100, Array(
      HistogramBin(lo = -50, hi = 0, ndv = 50),
      HistogramBin(lo = 0, hi = 50, ndv = 50),
      HistogramBin(lo = 50, hi = 100, ndv = 40),
      HistogramBin(lo = 100, hi = 150, ndv = 40)))

    // test bin trimming
    val (t1, h1) = trimBin(histogram1.bins(1), height = 300, min = 50, max = 75)
    assert(t1 == HistogramBin(lo = 50, hi = 60, ndv = 10) && h1 == 100)
    val (t2, h2) = trimBin(histogram1.bins(2), height = 300, min = 50, max = 75)
    assert(t2 == HistogramBin(lo = 60, hi = 75, ndv = 3) && h2 == 150)
    val (t3, h3) = trimBin(histogram2.bins(1), height = 100, min = 50, max = 75)
    assert(t3 == HistogramBin(lo = 50, hi = 50, ndv = 1) && h3 == 2)
    val (t4, h4) = trimBin(histogram2.bins(2), height = 100, min = 50, max = 75)
    assert(t4 == HistogramBin(lo = 50, hi = 75, ndv = 20) && h4 == 50)

    val expectedRanges = Seq(
      // t1 overlaps t3
      OverlappedRange(50, 50, 1, 1, 100/10, 2),
      // t1 overlaps t4
      OverlappedRange(50, 60, 10, 20*10/25, 100, 50*10/25),
      // t2 overlaps t4
      OverlappedRange(60, 75, 3, 20*15/25, 150, 50*15/25)
    )
    assert(expectedRanges.equals(
      getOverlappedRanges(histogram1, histogram2, newMin = 50D, newMax = 75D)))

    estimateByHistogram(
      histogram1 = histogram1,
      histogram2 = histogram2,
      expectedMin = 50D,
      expectedMax = 75D,
      // 1 + 8 + 3
      expectedNdv = 12L,
      // 10*2/1 + 100*20/10 + 150*30/12
      expectedRows = 595L)
  }

  test("cross join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    val join = Join(table1, table2, Cross, None)
    val expectedStats = Statistics(
      sizeInBytes = 5 * 3 * (8 + 4 * 4),
      rowCount = Some(5 * 3),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(
        Seq("key-1-5", "key-5-9", "key-1-2", "key-2-4").map(nameToColInfo)))
    assert(join.stats == expectedStats)
  }

  test("disjoint inner join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, Inner,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))))
    val expectedStats = Statistics(
      sizeInBytes = 1,
      rowCount = Some(0),
      attributeStats = AttributeMap(Nil))
    assert(join.stats == expectedStats)
  }

  test("disjoint left outer join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, LeftOuter,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))))
    val expectedStats = Statistics(
      sizeInBytes = 5 * (8 + 4 * 4),
      rowCount = Some(5),
      attributeStats = AttributeMap(Seq("key-1-5", "key-5-9").map(nameToColInfo) ++
        // Null count for right side columns = left row count
        Seq(nameToAttr("key-1-2") -> nullColumnStat(nameToAttr("key-1-2").dataType, 5),
          nameToAttr("key-2-4") -> nullColumnStat(nameToAttr("key-2-4").dataType, 5))))
    assert(join.stats == expectedStats)
  }

  test("disjoint right outer join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, RightOuter,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))))
    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      attributeStats = AttributeMap(Seq("key-1-2", "key-2-4").map(nameToColInfo) ++
        // Null count for left side columns = right row count
        Seq(nameToAttr("key-1-5") -> nullColumnStat(nameToAttr("key-1-5").dataType, 3),
          nameToAttr("key-5-9") -> nullColumnStat(nameToAttr("key-5-9").dataType, 3))))
    assert(join.stats == expectedStats)
  }

  test("disjoint full outer join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // key-5-9 and key-2-4 are disjoint
    val join = Join(table1, table2, FullOuter,
      Some(EqualTo(nameToAttr("key-5-9"), nameToAttr("key-2-4"))))
    val expectedStats = Statistics(
      sizeInBytes = (5 + 3) * (8 + 4 * 4),
      rowCount = Some(5 + 3),
      attributeStats = AttributeMap(
        // Update null count in column stats.
        Seq(nameToAttr("key-1-5") -> columnInfo(nameToAttr("key-1-5")).copy(nullCount = 3),
          nameToAttr("key-5-9") -> columnInfo(nameToAttr("key-5-9")).copy(nullCount = 3),
          nameToAttr("key-1-2") -> columnInfo(nameToAttr("key-1-2")).copy(nullCount = 5),
          nameToAttr("key-2-4") -> columnInfo(nameToAttr("key-2-4")).copy(nullCount = 5))))
    assert(join.stats == expectedStats)
  }

  test("inner join") {
    // table1 (key-1-5 int, key-5-9 int): (1, 9), (2, 8), (3, 7), (4, 6), (5, 5)
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    val join = Join(table1, table2, Inner,
      Some(EqualTo(nameToAttr("key-1-5"), nameToAttr("key-1-2"))))
    // Update column stats for equi-join keys (key-1-5 and key-1-2).
    val joinedColStat = ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
      avgLen = 4, maxLen = 4)
    // Update column stat for other column if #outputRow / #sideRow < 1 (key-5-9), or keep it
    // unchanged (key-2-4).
    val colStatForkey59 = nameToColInfo("key-5-9")._2.copy(distinctCount = 5 * 3 / 5)

    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      attributeStats = AttributeMap(
        Seq(nameToAttr("key-1-5") -> joinedColStat, nameToAttr("key-1-2") -> joinedColStat,
          nameToAttr("key-5-9") -> colStatForkey59, nameToColInfo("key-2-4"))))
    assert(join.stats == expectedStats)
  }

  test("inner join with multiple equi-join keys") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table2, table3, Inner, Some(
      And(EqualTo(nameToAttr("key-1-2"), nameToAttr("key-1-2")),
        EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3")))))

    // Update column stats for join keys.
    val joinedColStat1 = ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
        avgLen = 4, maxLen = 4)
    val joinedColStat2 = ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      attributeStats = AttributeMap(
        Seq(nameToAttr("key-1-2") -> joinedColStat1, nameToAttr("key-1-2") -> joinedColStat1,
          nameToAttr("key-2-4") -> joinedColStat2, nameToAttr("key-2-3") -> joinedColStat2)))
    assert(join.stats == expectedStats)
  }

  test("left outer join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table3, table2, LeftOuter,
      Some(EqualTo(nameToAttr("key-2-3"), nameToAttr("key-2-4"))))
    val joinedColStat = ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      // Keep the column stat from left side unchanged.
      attributeStats = AttributeMap(
        Seq(nameToColInfo("key-1-2"), nameToColInfo("key-2-3"),
          nameToColInfo("key-1-2"), nameToAttr("key-2-4") -> joinedColStat)))
    assert(join.stats == expectedStats)
  }

  test("right outer join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table2, table3, RightOuter,
      Some(EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3"))))
    val joinedColStat = ColumnStat(distinctCount = 2, min = Some(2), max = Some(3), nullCount = 0,
      avgLen = 4, maxLen = 4)

    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 * 4),
      rowCount = Some(2),
      // Keep the column stat from right side unchanged.
      attributeStats = AttributeMap(
        Seq(nameToColInfo("key-1-2"), nameToAttr("key-2-4") -> joinedColStat,
          nameToColInfo("key-1-2"), nameToColInfo("key-2-3"))))
    assert(join.stats == expectedStats)
  }

  test("full outer join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    val join = Join(table2, table3, FullOuter,
      Some(EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3"))))

    val expectedStats = Statistics(
      sizeInBytes = 3 * (8 + 4 * 4),
      rowCount = Some(3),
      // Keep the column stat from both sides unchanged.
      attributeStats = AttributeMap(Seq(nameToColInfo("key-1-2"), nameToColInfo("key-2-4"),
        nameToColInfo("key-1-2"), nameToColInfo("key-2-3"))))
    assert(join.stats == expectedStats)
  }

  test("left semi/anti join") {
    // table2 (key-1-2 int, key-2-4 int): (1, 2), (2, 3), (2, 4)
    // table3 (key-1-2 int, key-2-3 int): (1, 2), (2, 3)
    Seq(LeftSemi, LeftAnti).foreach { jt =>
      val join = Join(table2, table3, jt,
        Some(EqualTo(nameToAttr("key-2-4"), nameToAttr("key-2-3"))))
      // For now we just propagate the statistics from left side for left semi/anti join.
      val expectedStats = Statistics(
        sizeInBytes = 3 * (8 + 4 * 2),
        rowCount = Some(3),
        attributeStats = AttributeMap(Seq(nameToColInfo("key-1-2"), nameToColInfo("key-2-4"))))
      assert(join.stats == expectedStats)
    }
  }

  test("test join keys of different types") {
    /** Columns in a table with only one row */
    def genColumnData: mutable.LinkedHashMap[Attribute, ColumnStat] = {
      val dec = Decimal("1.000000000000000000")
      val date = DateTimeUtils.fromJavaDate(Date.valueOf("2016-05-08"))
      val timestamp = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-05-08 00:00:01"))
      mutable.LinkedHashMap[Attribute, ColumnStat](
        AttributeReference("cbool", BooleanType)() -> ColumnStat(distinctCount = 1,
          min = Some(false), max = Some(false), nullCount = 0, avgLen = 1, maxLen = 1),
        AttributeReference("cbyte", ByteType)() -> ColumnStat(distinctCount = 1,
          min = Some(1.toByte), max = Some(1.toByte), nullCount = 0, avgLen = 1, maxLen = 1),
        AttributeReference("cshort", ShortType)() -> ColumnStat(distinctCount = 1,
          min = Some(1.toShort), max = Some(1.toShort), nullCount = 0, avgLen = 2, maxLen = 2),
        AttributeReference("cint", IntegerType)() -> ColumnStat(distinctCount = 1,
          min = Some(1), max = Some(1), nullCount = 0, avgLen = 4, maxLen = 4),
        AttributeReference("clong", LongType)() -> ColumnStat(distinctCount = 1,
          min = Some(1L), max = Some(1L), nullCount = 0, avgLen = 8, maxLen = 8),
        AttributeReference("cdouble", DoubleType)() -> ColumnStat(distinctCount = 1,
          min = Some(1.0), max = Some(1.0), nullCount = 0, avgLen = 8, maxLen = 8),
        AttributeReference("cfloat", FloatType)() -> ColumnStat(distinctCount = 1,
          min = Some(1.0f), max = Some(1.0f), nullCount = 0, avgLen = 4, maxLen = 4),
        AttributeReference("cdec", DecimalType.SYSTEM_DEFAULT)() -> ColumnStat(distinctCount = 1,
          min = Some(dec), max = Some(dec), nullCount = 0, avgLen = 16, maxLen = 16),
        AttributeReference("cstring", StringType)() -> ColumnStat(distinctCount = 1,
          min = None, max = None, nullCount = 0, avgLen = 3, maxLen = 3),
        AttributeReference("cbinary", BinaryType)() -> ColumnStat(distinctCount = 1,
          min = None, max = None, nullCount = 0, avgLen = 3, maxLen = 3),
        AttributeReference("cdate", DateType)() -> ColumnStat(distinctCount = 1,
          min = Some(date), max = Some(date), nullCount = 0, avgLen = 4, maxLen = 4),
        AttributeReference("ctimestamp", TimestampType)() -> ColumnStat(distinctCount = 1,
          min = Some(timestamp), max = Some(timestamp), nullCount = 0, avgLen = 8, maxLen = 8)
      )
    }

    val columnInfo1 = genColumnData
    val columnInfo2 = genColumnData
    val table1 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq,
      rowCount = 1,
      attributeStats = AttributeMap(columnInfo1.toSeq))
    val table2 = StatsTestPlan(
      outputList = columnInfo2.keys.toSeq,
      rowCount = 1,
      attributeStats = AttributeMap(columnInfo2.toSeq))
    val joinKeys = table1.output.zip(table2.output)
    joinKeys.foreach { case (key1, key2) =>
      withClue(s"For data type ${key1.dataType}") {
        // All values in two tables are the same, so column stats after join are also the same.
        val join = Join(Project(Seq(key1), table1), Project(Seq(key2), table2), Inner,
          Some(EqualTo(key1, key2)))
        val expectedStats = Statistics(
          sizeInBytes = 1 * (8 + 2 * getColSize(key1, columnInfo1(key1))),
          rowCount = Some(1),
          attributeStats = AttributeMap(Seq(key1 -> columnInfo1(key1), key2 -> columnInfo1(key1))))
        assert(join.stats == expectedStats)
      }
    }
  }

  test("join with null column") {
    val (nullColumn, nullColStat) = (attr("cnull"),
      ColumnStat(distinctCount = 0, min = None, max = None, nullCount = 1, avgLen = 4, maxLen = 4))
    val nullTable = StatsTestPlan(
      outputList = Seq(nullColumn),
      rowCount = 1,
      attributeStats = AttributeMap(Seq(nullColumn -> nullColStat)))
    val join = Join(table1, nullTable, Inner, Some(EqualTo(nameToAttr("key-1-5"), nullColumn)))
    val expectedStats = Statistics(
      sizeInBytes = 1,
      rowCount = Some(0),
      attributeStats = AttributeMap(Nil))
    assert(join.stats == expectedStats)
  }
}
