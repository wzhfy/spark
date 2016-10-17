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

import com.google.common.primitives.{Ints, Longs}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.aggregate.IntervalDistinctApprox.HLLPPDispatcher
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, HyperLogLogPlusPlusAlgo}
import org.apache.spark.sql.types._

/**
 * The IntervalDistinctApprox function counts the approximate number of distinct values (ndv) in
 * intervals constructed from endpoints specified in `endpointsExpression`.
 * To count ndv's in these intervals, apply the HyperLogLogPlusPlus algorithm in each of them.
 * @param child to estimate the ndv's of.
 * @param endpointsExpression to construct the intervals.
 * @param relativeSD The maximum estimation error allowed in the HyperLogLogPlusPlus algorithm.
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(col, array(endpoint_1, endpoint_2, ... endpoint_N)) - Returns the approximate
      number of distinct values (ndv) for intervals [endpoint_1, endpoint_2],
      (endpoint_2, endpoint_3], ... (endpoint_N-1, endpoint_N].

      _FUNC_(col, array(endpoint_1, endpoint_2, ... endpoint_N), relativeSD=0.05) - Returns
      the approximate number of distinct values (ndv) for intervals with relativeSD, the maximum
      estimation error allowed in the HyperLogLogPlusPlus algorithm.
    """)
case class IntervalDistinctApprox(
    child: Expression,
    endpointsExpression: Expression,
    relativeSD: Double = 0.05,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedImperativeAggregate[HLLPPDispatcher] {

  def this(child: Expression, endpointsExpression: Expression) = {
    this(
      child = child,
      endpointsExpression = endpointsExpression,
      relativeSD = 0.05,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  def this(child: Expression, endpointsExpression: Expression, relativeSD: Expression) = {
    this(
      child = child,
      endpointsExpression = endpointsExpression,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(child.dataType, TypeCollection(DoubleType, ArrayType))
  }

  // Mark as lazy so that intervalExpression is not evaluated during tree transformation.
  private lazy val endpoints: Array[Double] = {
    val doubleArray = (endpointsExpression.dataType, endpointsExpression.eval()) match {
      case (ArrayType(baseType: NumericType, _), arrayData: ArrayData) =>
        val numericArray = arrayData.toObjectArray(baseType)
        numericArray.map { x =>
          baseType.numeric.toDouble(x.asInstanceOf[baseType.InternalType])
        }
      case other =>
        throw new AnalysisException(s"Invalid data type ${other._1} for parameter endpoints")
    }
    util.Arrays.sort(doubleArray)
    doubleArray
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!child.dataType.isInstanceOf[NumericType] &&
      !child.dataType.isInstanceOf[DateType] && !child.dataType.isInstanceOf[TimestampType]) {
      TypeCheckFailure("The interval_distinct_approx function only supports data type which is " +
        "stored internally as Numeric, but the data type of ${child.prettyName} is " +
        s"${child.dataType}")
    } else if (!endpointsExpression.foldable) {
      TypeCheckFailure("The intervals provided must be constant literals")
    } else if (endpoints.length < 2) {
      TypeCheckFailure("The number of endpoints must be >= 2 to construct intervals")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): HLLPPDispatcher = {
    // N endpoints construct N-1 intervals, creating a HLLPP for each interval
    val hllppArray = new Array[HyperLogLogPlusPlusAlgo](endpoints.length - 1)
    for (i <- endpoints.indices) {
      hllppArray(i) = new HyperLogLogPlusPlusAlgo(relativeSD)
    }
    new HLLPPDispatcher(endpoints, hllppArray)
  }

  override def update(buffer: HLLPPDispatcher, inputRow: InternalRow): Unit = {
    val value = child.eval(inputRow)
    // Ignore empty rows
    if (value != null) {
      buffer.insert(value, child.dataType)
    }
  }

  override def merge(buffer: HLLPPDispatcher, other: HLLPPDispatcher): Unit = {
    buffer.merge(other)
  }

  override def eval(buffer: HLLPPDispatcher): Any = {
    new GenericArrayData(buffer.getResults)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): IntervalDistinctApprox =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): IntervalDistinctApprox =
    copy(inputAggBufferOffset = newOffset)

  override def children: Seq[Expression] = Seq(child, endpointsExpression)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(LongType)

  override def prettyName: String = "interval_distinct_approx"

  override def serialize(obj: HLLPPDispatcher): Array[Byte] = {
    IntervalDistinctApprox.serializer.serialize(obj)
  }

  override def deserialize(bytes: Array[Byte]): HLLPPDispatcher = {
    IntervalDistinctApprox.serializer.deserialize(bytes)
  }
}

object IntervalDistinctApprox {

  case class HLLPPDispatcher(endpoints: Array[Double], hllpps: Array[HyperLogLogPlusPlusAlgo]) {

    // Find which HyperLogLogPlusPlusAlgo should receive the given value.
    def insert(value: Any, dataType: DataType): Unit = {
      // convert the value into a double value for searching in the double array
      val doubleValue = dataType match {
        case n: NumericType =>
          n.numeric.toDouble(value.asInstanceOf[n.InternalType])
        case d: DateType =>
          value.asInstanceOf[Int].toDouble
        case t: TimestampType =>
          value.asInstanceOf[Long].toDouble
      }

      // endpoints are sorted already
      if (endpoints.head > doubleValue || endpoints.last < doubleValue) {
        // ignore if the value is out of the whole range
        return
      }
      var index = util.Arrays.binarySearch(endpoints, doubleValue)
      if (index >= 0) {
        // The value is found.
        if (index == 0) {
          hllpps(0).insert(value, dataType)
        } else {
          // If the endpoints contains multiple elements with the specified value, there is no
          // guarantee which one binarySearch will return. We remove this uncertainty by moving the
          // index to the first position of these elements.
          var first = index - 1
          while(first >= 0 && endpoints(first) == value) {
            first -= 1
          }
          index = first + 1

          // send values in (endpoints(index-1), endpoints(index)] to hllpps(index-1)
          hllpps(index - 1).insert(value, dataType)
        }
      } else {
        // The value is not found, binarySearch returns (-(<i>insertion point</i>) - 1).
        // The <i>insertion point</i> is defined as the point at which the key would be inserted
        // into the array: the index of the first element greater than the key.
        val insertionPoint = - (index + 1)
        hllpps(insertionPoint - 1).insert(value, dataType)
      }
    }

    def merge(other: HLLPPDispatcher): Unit = {
      assert(endpoints.sameElements(other.endpoints))
      assert(hllpps.length == other.hllpps.length)
      for (i <- hllpps.indices) {
        hllpps(i).merge(other.hllpps(i))
      }
    }

    def getResults: Array[Long] = {
      val ndvArray = hllpps.map(_.query)
      // If the endpoints contains multiple elements with the same value,
      // we set ndv=1 for intervals between these elements.
      // E.g. given four endpoints (1, 2, 2, 4) and input sequence (0.5, 2),
      // the ndv's for the three intervals should be (2, 1, 0)
      for (i <- 0 until endpoints.length - 2) {
        if (endpoints(i) == endpoints(i + 1)) ndvArray(i) = 1
      }
      ndvArray
    }
  }

  class HLLPPDispatcherSerializer {

    private final def length(obj: HLLPPDispatcher): Int = {
      // obj.endpoints.length, obj.endpoints, obj.hllpps.length
      var len = Ints.BYTES + obj.endpoints.length * Longs.BYTES + obj.hllpps.length
      // obj.hllpps
      obj.hllpps.foreach(hllpp => len += HyperLogLogPlusPlusAlgo.length(hllpp))
      len
    }

    final def serialize(obj: HLLPPDispatcher): Array[Byte] = {
      val buffer = ByteBuffer.wrap(new Array(length(obj)))
      buffer.putInt(obj.endpoints.length)
      obj.endpoints.foreach(buffer.putDouble)
      buffer.putInt(obj.hllpps.length)
      obj.hllpps.foreach(hllpp => buffer.put(HyperLogLogPlusPlusAlgo.serialize(hllpp)))
      buffer.array()
    }

    final def deserialize(bytes: Array[Byte]): HLLPPDispatcher = {
      val buffer = ByteBuffer.wrap(bytes)
      val endpointsLength = buffer.getInt
      val endpoints = new Array[Double](endpointsLength)
      for (i <- 0 until endpointsLength) endpoints(i) = buffer.getDouble

      val hllppNumber = buffer.getInt
      val hllpps = new Array[HyperLogLogPlusPlusAlgo](hllppNumber)
      for (i <- 0 until hllppNumber) hllpps(i) = HyperLogLogPlusPlusAlgo.deserialize(buffer)

      new HLLPPDispatcher(endpoints, hllpps)
    }
  }

  val serializer: HLLPPDispatcherSerializer = new HLLPPDispatcherSerializer
}
