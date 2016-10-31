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

package org.apache.spark.sql.catalyst.plans.logical

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Estimates of various statistics.  The default estimation logic simply lazily multiplies the
 * corresponding statistic produced by the children.  To override this behavior, override
 * `statistics` and assign it an overridden version of `Statistics`.
 *
 * '''NOTE''': concrete and/or overridden versions of statistics fields should pay attention to the
 * performance of the implementations.  The reason is that estimations might get triggered in
 * performance-critical processes, such as query plan planning.
 *
 * Note that we are using a BigInt here since it is easy to overflow a 64-bit integer in
 * cardinality estimation (e.g. cartesian joins).
 *
 * @param sizeInBytes Physical size in bytes. For leaf operators this defaults to 1, otherwise it
 *                    defaults to the product of children's `sizeInBytes`.
 * @param rowCount Estimated number of rows.
 * @param colStats Column-level statistics.
 * @param isBroadcastable If true, output is small enough to be used in a broadcast join.
 */
case class Statistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, ColumnStat] = Map.empty,
    isBroadcastable: Boolean = false) {

  override def toString: String = "Statistics(" + simpleString + ")"

  /** Readable string representation for the Statistics. */
  def simpleString: String = {
    Seq(s"sizeInBytes=$sizeInBytes",
      if (rowCount.isDefined) s"rowCount=${rowCount.get}" else "",
      s"isBroadcastable=$isBroadcastable"
    ).filter(_.nonEmpty).mkString(", ")
  }
}

// Statistics for a column.
trait ColumnStat

// This ColumnStat is for interaction with metastore. It's only in relation nodes (LeafNodes).
case class LeafColumnStat(statRow: InternalRow) extends ColumnStat{

  def forNumeric[T <: AtomicType](dataType: T): NumericColumnStat[T] = {
    // The indices here must be consistent with `ColumnStatStruct.numericColumnStat`.
    val numNulls: Long = statRow.getLong(0)
    val max: T#InternalType = statRow.get(1, dataType).asInstanceOf[T#InternalType]
    val min: T#InternalType = statRow.get(2, dataType).asInstanceOf[T#InternalType]
    val ndv: Long = statRow.getLong(3)
    val histogram: Option[Histogram] = if (statRow.isNullAt(4)) {
      // all records are null
      None
    } else {
      val mapData = statRow.getMap(4)
      if (mapData.numElements() == 0) {
        // ndv exceeds the maximum number of bins in a histogram, construct an equi-height histogram
        if (!statRow.isNullAt(5)) {
          val percentiles = statRow.getArray(5).toDoubleArray()
        }
      } else {
        // construct an equi-width histogram
        val keys: Array[Double] = mapData.keyArray().toDoubleArray()
        val values: Array[Long] = mapData.valueArray().toLongArray()
        val bins = keys.zip(values).map(e => NumericEquiWidthBin(e._1, e._2))
        Some(NumericEquiWidthHgm(bins))
      }
    }
    NumericColumnStat(numNulls, max, min, ndv, histogram)
  }

  def forString: StringColumnStat = {
    // The indices here must be consistent with `ColumnStatStruct.stringColumnStat`.
    val numNulls: Long = statRow.getLong(0)
    val avgColLen: Double = statRow.getDouble(1)
    val maxColLen: Int = statRow.getInt(2)
    val ndv: Long = statRow.getLong(3)
    val histogram: Option[StringEquiWidthHgm] = if (statRow.isNullAt(4)) {
      // all records are null
      None
    } else {
      val mapData = statRow.getMap(4)
      if (mapData.numElements() == 0) {
        // ndv exceeds the maximum number of bins in a histogram
        None
      } else {
        // construct an equi-width histogram
        val keys: Array[UTF8String] = mapData.keyArray().toArray(StringType)
        val values: Array[Long] = mapData.valueArray().toLongArray()
        val bins = keys.zip(values).map(e => StringEquiWidthBin(e._1, e._2))
        Some(StringEquiWidthHgm(bins))
      }
    }
    StringColumnStat(numNulls, avgColLen, maxColLen, ndv, histogram)
  }

  def forBinary: BinaryColumnStat = {
    // The indices here must be consistent with `ColumnStatStruct.binaryColumnStat`.
    val numNulls: Long = statRow.getLong(0)
    val avgColLen: Double = statRow.getDouble(1)
    val maxColLen: Int = statRow.getInt(2)
    BinaryColumnStat(numNulls, avgColLen, maxColLen)
  }

  def forBoolean: BooleanColumnStat = {
    // The indices here must be consistent with `ColumnStatStruct.booleanColumnStat`.
    val numNulls: Long = statRow.getLong(0)
    val numTrues: Long = statRow.getLong(1)
    val numFalses: Long = statRow.getLong(2)
    BooleanColumnStat(numNulls, numTrues, numFalses)
  }

  override def toString: String = {
    // use Base64 for encoding
    Base64.encodeBase64String(statRow.asInstanceOf[UnsafeRow].getBytes)
  }
}

object LeafColumnStat {
  def apply(numFields: Int, str: String): LeafColumnStat = {
    // use Base64 for decoding
    val bytes = Base64.decodeBase64(str)
    val unsafeRow = new UnsafeRow(numFields)
    unsafeRow.pointTo(bytes, bytes.length)
    LeafColumnStat(unsafeRow)
  }
}

// These four classes are for estimation. They are only in non-leaf nodes.
case class NumericColumnStat[T <: AtomicType](numNulls: Long, max: T#InternalType,
    min: T#InternalType, ndv: Long, histogram: Option[Histogram]) extends ColumnStat

case class StringColumnStat(numNulls: Long, avgColLen: Double, maxColLen: Int, ndv: Long,
    histogram: Option[StringEquiWidthHgm]) extends ColumnStat

case class BinaryColumnStat(numNulls: Long, avgColLen: Double, maxColLen: Int) extends ColumnStat

case class BooleanColumnStat(numNulls: Long, numTrues: Long, numFalses: Long) extends ColumnStat


trait Histogram
trait EquiWidthHistogram extends Histogram
trait EquiHeightHistogram extends Histogram

/**
 * Equi-width histogram for numeric type, which consists of an array of single valued bins.
 * We use double type to represent all numeric types for simplicity in estimation.
 */
case class NumericEquiWidthHgm(bins: Array[NumericEquiWidthBin]) extends EquiWidthHistogram

/**
 * Equi-width histogram for string type, which consists of an array of single valued bins.
 */
case class StringEquiWidthHgm(bins: Array[StringEquiWidthBin]) extends EquiWidthHistogram

/**
 * Equi-height histogram for numeric type.
 *
 * @param bins An array of equi-height bins.
 * @param binFrequency The height of this histogram, i.e. total frequency of the values in each
 *                        bin.
 */
case class NumericEquiHeightHgm(bins: Array[NumericEquiHeightBin], binFrequency: BigDecimal)
  extends EquiHeightHistogram

/**
 * A single valued bin in an equi-width histogram for numeric type.
 *
 * @param value Value of this bin.
 * @param frequency Frequency of this value.
 */
case class NumericEquiWidthBin(value: Double, frequency: Long)

/**
 * A single valued bin in an equi-width histogram for string type.
 *
 * @param value Value of this bin.
 * @param frequency Frequency of this value.
 */
case class StringEquiWidthBin(value: UTF8String, frequency: Long)

/**
 * A bin in an equi-height histogram for numeric type.
 *
 * @param lo Value of the lower bound of this bin.
 * @param up Value of the upper bound end of this bin.
 * @param binNdv NDV in this bin.
 */
case class NumericEquiHeightBin(lo: Double, up: Double, binNdv: Long)
