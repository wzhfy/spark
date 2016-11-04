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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.nio.ByteBuffer
import java.util

import scala.collection.mutable

import com.google.common.primitives.{Doubles, Ints, Longs}
import org.apache.spark.serializer.{KryoSerializer, KryoSerializerInstance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This function computes frequency for each distinct non-null value of a column.
 * It returns: 1. null if the table is empty or all values of the column are null.
 * 2. (distinct non-null value, frequency) pairs if the number of distinct non-null values is
 * less than or equal to the specified threshold.
 * 3. an empty result if the number of distinct non-null values exceeds that threshold.
 *
 * @param child child expression that can produce column value with `child.eval(inputRow)`
 * @param numBinsExpression a threshold of number of distinct non-null values, beyond which the
 *                          function will return an empty result
 */
@ExpressionDescription(
  usage = """
    _FUNC_(col, numBins) - Computes frequency for each distinct non-null value of column `col`.
      It returns: 1. null if the table is empty or all values of column `col` are null.
      2. (distinct non-null value, frequency) pairs if the number of distinct non-null values
      is less than or equal to the specified threshold `numBins`.
      3. an empty result if the number of distinct non-null values exceeds `numBins`.
  """,
  extended = """
    Examples:
      > SELECT map_aggregate(col, 3) FROM tbl;
       1. null - if `tbl` is empty or values of `col` are all nulls
       2. Map((10, 2), (20, 1)) - if values of `col` are (10, 20, 10)
       3. Map.empty - if values of `col` are (1, 2, 3, 4)
  """)
case class MapAggregate(
    child: Expression,
    numBinsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int) extends TypedImperativeAggregate[MapDigest] {

  def this(child: Expression, numBinsExpression: Expression) = {
    this(child, numBinsExpression, 0, 0)
  }

  /** Mark as lazy so that numBinsExpression is not evaluated during tree transformation. */
  private lazy val numBins: Int = numBinsExpression.eval().asInstanceOf[Int]

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(NumericType, TimestampType, DateType, StringType), IntegerType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!numBinsExpression.foldable) {
      TypeCheckFailure("The maximum number of bins provided must be a literal or constant foldable")
    } else if (numBins < 2) {
      val currentValue = if (numBinsExpression.eval() == null) null else numBins
      TypeCheckFailure(
        "The maximum number of bins provided must be a positive integer literal >= 2 " +
          s"(current value = $currentValue)")
    } else {
      TypeCheckSuccess
    }
  }

  override def update(buffer: MapDigest, input: InternalRow): Unit = {
    if (!buffer.isInvalid) {
      val evaluated = child.eval(input)
      if (evaluated != null) buffer.update(child.dataType, evaluated, numBins)
    }
  }

  override def merge(buffer: MapDigest, other: MapDigest): Unit = {
    if (!buffer.isInvalid) {
      if (other.isInvalid) {
        buffer.isInvalid = true
        buffer.clear()
      } else {
        buffer.merge(other, numBins)
      }
    }
  }

  override def eval(buffer: MapDigest): Any = {
    if (buffer.isInvalid) {
      // return empty map
      ArrayBasedMapData(Map.empty)
    } else {
      // sort the result to make it easy to use in histograms and readable to users
      val sorted = buffer match {
        case stringDigest: StringMapDigest => new util.TreeMap(stringDigest.bins)
        case numericDigest: NumericMapDigest => new util.TreeMap(numericDigest.bins)
      }
      if (sorted.isEmpty) {
        // don't have non-null values
        null
      } else {
        ArrayBasedMapData(sorted.keySet().toArray, sorted.values.toArray)
      }
    }
  }

  override def serialize(buffer: MapDigest): Array[Byte] = {
    val serializer = new KryoSerializerInstance()
    buffer.serialize()
  }

  override def deserialize(bytes: Array[Byte]): MapDigest = {
    MapDigest.deserialize(child.dataType, bytes)
  }

  override def createAggregationBuffer(): MapDigest = {
    child.dataType match {
      case StringType => StringMapDigest()
      case _ => NumericMapDigest()
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MapAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MapAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = {
    child.dataType match {
      case StringType => MapType(StringType, LongType)
      case _ => MapType(DoubleType, LongType)
    }
  }

  override def children: Seq[Expression] = Seq(child, numBinsExpression)

  override def prettyName: String = "map_aggregate"
}

/**
 *  This class maintains a map consisting of distinct values and their counts.
 */
trait MapDigest {
  /** Mark this MapDigest invalid when the size of the map exceeds numBins */
  var isInvalid: Boolean
  def update(dataType: DataType, value: Any, numBins: Int): Unit
  def merge(otherDigest: MapDigest, numBins: Int): Unit
  def clear(): Unit
  def serialize(): Array[Byte]
}

abstract class MapDigestBase[T] extends MapDigest {
  /** HashMap[value of internal type T, count of the value] */
  val bins: util.HashMap[T, Long]

  /** Update bins and clear it when its size exceeds numBins. */
  def updateMap(key: T, value: Long, numBins: Int): Unit = {
    if (bins.containsKey(key)) {
      bins.put(key, bins.get(key) + value)
    } else {
      if (bins.size() >= numBins) {
        isInvalid = true
        bins.clear()
      } else {
        bins.put(key, value)
      }
    }
  }

  /** Merge two maps and clear bins when its size exceeds numBins. */
  override def merge(otherDigest: MapDigest, numBins: Int): Unit = {
    val otherMap = otherDigest.asInstanceOf[MapDigestBase[T]].bins
    val otherEntries = otherMap.entrySet().iterator()
    while (!isInvalid && otherEntries.hasNext) {
      val entry = otherEntries.next()
      updateMap(entry.getKey, entry.getValue, numBins)
    }
  }

  override def clear(): Unit = bins.clear()

  override def serialize(): Array[Byte] = {
    // isInvalid, size of bins
    var length: Int = Ints.BYTES + Ints.BYTES
    bins.foreach { case (key, value) =>
      // key, value
      length += keyLen(key) + Longs.BYTES
    }
    val buffer = ByteBuffer.wrap(new Array(length))
    buffer.putInt(if (isInvalid) 0 else 1)
    buffer.putInt(bins.size)
    bins.foreach { case (key, value) =>
      putKey(key, buffer)
      buffer.putLong(value)
    }
    buffer.array()
  }

  def deserialize(bytes: Array[Byte]): (util.HashMap[T, Long], Boolean) = {
    val buffer = ByteBuffer.wrap(bytes)
    val isInvalid = if (buffer.getInt == 0) true else false
    val size = buffer.getInt
    val bins = new util.HashMap[T, Long]
    var i = 0
    while (i < size) {
      val key = getKey(buffer)
      val value = buffer.getLong
      bins.put(key, value)
      i += 1
    }
    (bins, isInvalid)
  }

  def keyLen(key: T): Int
  def putKey(key: T, buffer: ByteBuffer): Unit
  def getKey(buffer: ByteBuffer): T
}

object MapDigest {
  def deserialize(dataType: DataType, bytes: Array[Byte]): MapDigest = {
    dataType match {
      case StringType =>
        val (bins, isInvalid) = StringMapDigest().deserialize(bytes)
        new StringMapDigest(bins, isInvalid)
      case _ =>
        val (bins, isInvalid) = NumericMapDigest().deserialize(bytes)
        new NumericMapDigest(bins, isInvalid)
    }
  }
}

case class StringMapDigest(
    override val bins: util.HashMap[UTF8String, Long] = new util.HashMap(),
    override var isInvalid: Boolean = false) extends MapDigestBase[UTF8String] {

  override def update(dataType: DataType, value: Any, numBins: Int): Unit = {
    updateMap(value.asInstanceOf[UTF8String], numBins)
  }

  override def keyLen(key: UTF8String): Int = Ints.BYTES + key.getBytes.length

  override def putKey(key: UTF8String, buffer: ByteBuffer): Unit = {
    val bytes = key.getBytes
    buffer.putInt(bytes.length)
    buffer.put(bytes)
  }

  override def getKey(buffer: ByteBuffer): UTF8String = {
    val keyLength = buffer.getInt
    var i = 0
    val keyBytes = new Array[Byte](keyLength)
    while (i < keyLength) {
      keyBytes(i) = buffer.get()
      i += 1
    }
    UTF8String.fromBytes(keyBytes)
  }
}

case class NumericMapDigest(
    override val bins: util.HashMap[Double, Long] = new util.HashMap(),
    override var isInvalid: Boolean = false) extends MapDigestBase[Double] {

  override def update(dataType: DataType, value: Any, numBins: Int): Unit = {
    // use Double to represent endpoints (in histograms) for simplicity
    val doubleValue = dataType match {
      case n: NumericType =>
        n.numeric.toDouble(value.asInstanceOf[n.InternalType])
      case d: DateType =>
        value.asInstanceOf[Int].toDouble
      case t: TimestampType =>
        value.asInstanceOf[Long].toDouble
    }
    updateMap(doubleValue, numBins)
  }

  override def keyLen(key: Double): Int = Doubles.BYTES

  override def putKey(key: Double, buffer: ByteBuffer): Unit = buffer.putDouble(key)

  override def getKey(buffer: ByteBuffer): Double = buffer.getDouble
}
