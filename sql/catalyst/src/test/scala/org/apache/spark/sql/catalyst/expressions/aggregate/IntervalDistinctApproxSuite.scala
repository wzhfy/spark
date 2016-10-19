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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, CreateArray, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.IntervalDistinctApprox.{HLLPPDispatcher, HLLPPDispatcherSerializer}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DoubleType, StringType}

class IntervalDistinctApproxSuite extends SparkFunSuite {

  private val endpoints = Array[Double](0, 0.33, 0.6, 0.6, 0.6, 1.0)
  private val data = Seq(0, 0.6, 0.3, 1, 0.6, 0.5, 0.6, 0.33)
  private val expectedNdvs = Array[Long](3, 2, 1, 1, 1)

  test("serialize and de-serialize") {
    def assertEqualDispatcher(left: HLLPPDispatcher, right: HLLPPDispatcher): Unit = {
      assert(left.endpoints.sameElements(right.endpoints))
      assert(left.hllpps.length == right.hllpps.length)
      for (i <- left.hllpps.indices) {
        val h1 = left.hllpps(i)
        val h2 = right.hllpps(i)
        assert(h1.relativeSD == h2.relativeSD)
        assert(h1.words.sameElements(h2.words))
      }
    }

    val serializer = new HLLPPDispatcherSerializer
    val relativeSD: Double = 0.01
    // Check empty serialize and de-serialize
    val buffer = HLLPPDispatcher(endpoints, relativeSD)
    assertEqualDispatcher(buffer, serializer.deserialize(serializer.serialize(buffer)))

    val random = new java.util.Random()
    val data = (1 until 10000).map { _ =>
      random.nextDouble()
    }

    data.foreach { value =>
      buffer.insert(value, DoubleType)
    }
    assertEqualDispatcher(buffer, serializer.deserialize(serializer.serialize(buffer)))

    val agg = new IntervalDistinctApprox(BoundReference(0, DoubleType, nullable = true),
      CreateArray(endpoints.map(Literal(_))), Literal(relativeSD))
    assertEqualDispatcher(agg.deserialize(agg.serialize(buffer)), buffer)
  }

  test("class HLLPPDispatcher, basic operations") {
    Seq(0.01, 0.05, 0.1).foreach { relativeSD =>
      val buffer = HLLPPDispatcher(endpoints, relativeSD)
      data.grouped(4).foreach { group =>
        val partialBuffer = HLLPPDispatcher(endpoints, relativeSD)
        group.foreach(x => partialBuffer.insert(x, DoubleType))
        buffer.merge(partialBuffer)
      }
      // before eval(), for intervals with the same endpoints, only the first interval counts the
      // value
      checkNDVs(
        ndvs = buffer.hllpps.map(_.query),
        expectedNdvs = Array(3, 2, 0, 0, 1),
        rsd = relativeSD)
      // after eval(), set the others to 1
      checkNDVs(
        ndvs = buffer.getResults,
        expectedNdvs = expectedNdvs,
        rsd = relativeSD)
    }
  }

  test("class IntervalDistinctApprox, high level interface, update, merge, eval...") {
    val childExpression = BoundReference(0, DoubleType, nullable = false)
    val endpointsExpression = CreateArray(endpoints.reverse.map(Literal(_)))
    val agg = new IntervalDistinctApprox(childExpression, endpointsExpression)
    // check if the endpoints are sorted in an ascending order
    assert(agg.createAggregationBuffer().endpoints.sameElements(endpoints))

    assert(!agg.nullable)
    val group1 = 0 until data.length / 2
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group1Buffer, input)
    }

    val group2 = data.length / 2 until data.length
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    val results = agg.eval(mergeBuffer).asInstanceOf[ArrayData]
    checkNDVs(ndvs = results.toLongArray(), expectedNdvs = expectedNdvs, agg.relativeSD)
  }

  test("class IntervalDistinctApprox, low level interface, update, merge, eval...") {
    val childExpression = BoundReference(0, DoubleType, nullable = true)
    val endpointsExpression = CreateArray(endpoints.map(Literal(_)))
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2

    // Phase one, partial mode aggregation
    val agg = new IntervalDistinctApprox(childExpression, endpointsExpression)
      .withNewInputAggBufferOffset(inputAggregationBufferOffset)
      .withNewMutableAggBufferOffset(mutableAggregationBufferOffset)

    val mutableAggBuffer = new GenericInternalRow(
      new Array[Any](mutableAggregationBufferOffset + 1))
    agg.initialize(mutableAggBuffer)
    data.foreach(d => agg.update(mutableAggBuffer, InternalRow(d)))
    agg.serializeAggregateBufferInPlace(mutableAggBuffer)

    // Serialize the aggregation buffer
    val serialized = mutableAggBuffer.getBinary(mutableAggregationBufferOffset)
    val inputAggBuffer = new GenericInternalRow(Array[Any](null, serialized))

    // Phase 2: final mode aggregation
    // Re-initialize the aggregation buffer
    agg.initialize(mutableAggBuffer)
    agg.merge(mutableAggBuffer, inputAggBuffer)

    val results = agg.eval(mutableAggBuffer).asInstanceOf[ArrayData]
    checkNDVs(ndvs = results.toLongArray(), expectedNdvs = expectedNdvs, agg.relativeSD)
  }

  private def checkNDVs(ndvs: Array[Long], expectedNdvs: Array[Long], rsd: Double): Unit = {
    assert(ndvs.length == expectedNdvs.length)
    for (i <- ndvs.indices) {
      val ndv = ndvs(i)
      val expectedNdv = expectedNdvs(i)
      if (expectedNdv == 0) {
        assert(ndv == 0)
      } else if (expectedNdv > 0) {
        assert(ndv > 0)
        val error = math.abs((ndv / expectedNdv.toDouble) - 1.0d)
        assert(error <= rsd * 3.0d, "Error should be within 3 std. errors.")
      }
    }
  }

  test("class IntervalDistinctApprox, fails analysis if parameters are invalid") {
    val wrongColumn = new IntervalDistinctApprox(
      AttributeReference("a", StringType)(),
      endpointsExpression = CreateArray(endpoints.map(Literal(_))))
    assert(
      wrongColumn.checkInputDataTypes() match {
        case TypeCheckFailure(msg)
          if msg.contains("requires (numeric or timestamp or date) type") => true
        case _ => false
      })

    var wrongEndpoints = new IntervalDistinctApprox(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = Literal(0.5d))
    assert(
      wrongEndpoints.checkInputDataTypes() match {
        case TypeCheckFailure(msg) if msg.contains("requires array type") => true
        case _ => false
      })

    wrongEndpoints = new IntervalDistinctApprox(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = CreateArray(Seq(AttributeReference("b", DoubleType)())))
    assertEqual(
      wrongEndpoints.checkInputDataTypes(),
      TypeCheckFailure("The intervals provided must be constant literals"))

    wrongEndpoints = new IntervalDistinctApprox(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = CreateArray(Array(10L).map(Literal(_))))
    assertEqual(
      wrongEndpoints.checkInputDataTypes(),
      TypeCheckFailure("The number of endpoints must be >= 2 to construct intervals"))
  }

  private def assertEqual[T](left: T, right: T): Unit = {
    assert(left == right)
  }
}
