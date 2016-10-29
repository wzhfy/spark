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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._


class MapAggregateQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val table = "map_aggregate_test"
  private val col1 = "col1"
  private val col2 = "col2"
  private def query(numBins: Int): DataFrame = {
    sql(s"SELECT map_aggregate($col1, $numBins), map_aggregate($col2, $numBins) FROM $table")
  }

  test("null handling") {
    withTempView(table) {
      val schema = StructType(Seq(StructField(col1, StringType), StructField(col2, DoubleType)))
      // Empty input row
      val rdd1 = spark.sparkContext.parallelize(Seq(Row(null, null)))
      spark.createDataFrame(rdd1, schema).createOrReplaceTempView(table)
      checkAnswer(query(numBins = 2), Row(Map.empty, Map.empty))

      // Add some non-empty row
      val rdd2 = spark.sparkContext.parallelize(Seq(Row(null, 3.0D), Row("a", null)))
      spark.createDataFrame(rdd2, schema).createOrReplaceTempView(table)
      checkAnswer(query(numBins = 2), Row(Map(("a", 1)), Map((3.0D, 1))))
    }
  }

  test("returns empty result when ndv exceeds numBins") {
    withTempView(table) {
      spark.sparkContext.makeRDD(
        Seq(("a", 4), ("d", 2), ("c", 4), ("b", 1), ("a", 3), ("a", 2)), 2).toDF(col1, col2)
        .createOrReplaceTempView(table)
      checkAnswer(query(numBins = 4), Row(
        Map(("a", 3), ("b", 1), ("c", 1), ("d", 1)),
        Map((1.0D, 1), (2.0D, 2), (3.0D, 1), (4.0D, 2))))
      // One partial exceeds numBins during update()
      checkAnswer(query(numBins = 2), Row(Map.empty, Map.empty))
      // Exceeding numBins during merge()
      checkAnswer(query(numBins = 3), Row(Map.empty, Map.empty))
    }
  }

  test("multiple columns of different types") {
    def queryMultiColumns(numBins: Int): DataFrame = {
      sql(
        s"""
           |SELECT
           |  map_aggregate(c1, $numBins),
           |  map_aggregate(c2, $numBins),
           |  map_aggregate(c3, $numBins),
           |  map_aggregate(c4, $numBins),
           |  map_aggregate(c5, $numBins),
           |  map_aggregate(c6, $numBins),
           |  map_aggregate(c7, $numBins),
           |  map_aggregate(c8, $numBins),
           |  map_aggregate(c9, $numBins),
           |  map_aggregate(c10, $numBins)
           |FROM $table
        """.stripMargin)
    }

    val ints = Seq(5, 3, 1)
    val doubles = Seq(1.0D, 3.0D, 5.0D)
    val dates = Seq("1970-01-01", "1970-02-02", "1970-03-03")
    val timestamps = Seq("1970-01-01 00:00:00", "1970-01-01 00:00:05", "1970-01-01 00:00:10")
    val strings = Seq("a", "bb", "ccc")

    val data = ints.indices.map { i =>
      (ints(i).toByte,
        ints(i).toShort,
        ints(i),
        ints(i).toLong,
        doubles(i).toFloat,
        doubles(i),
        Decimal(doubles(i)),
        Date.valueOf(dates(i)),
        Timestamp.valueOf(timestamps(i)),
        strings(i))
    }
    withTempView(table) {
      data.toDF("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10")
        .createOrReplaceTempView(table)
      val expected1 = ArrayBuffer.empty[Map[Any, Long]]
      val frequency = Seq(1L, 1L, 1L)
      for (i <- 1 to 7) {
        expected1 += doubles.zip(frequency).toMap
      }
      expected1 += dates.map { d =>
        DateTimeUtils.fromJavaDate(Date.valueOf(d)).toDouble
      }.zip(frequency).toMap
      expected1 += timestamps.map { t =>
        DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(t)).toDouble
      }.zip(frequency).toMap
      expected1 += strings.zip(frequency).toMap
      checkAnswer(queryMultiColumns(ints.length), Row(expected1: _*))
    }
  }
}
