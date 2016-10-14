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

import com.google.common.primitives.{Doubles, Ints, Longs}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus.HLLPPDigest
import org.apache.spark.sql.catalyst.util.HyperLogLogPlusPlusAlgo
import org.apache.spark.sql.types._

// scalastyle:off
/**
 * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm. This class
 * implements the dense version of the HLL++ algorithm as an Aggregate Function.
 *
 * This implementation has been based on the following papers:
 * HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation
 * Algorithm
 * http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf
 *
 * Appendix to HyperLogLog in Practice: Algorithmic Engineering of a State of the Art Cardinality
 * Estimation Algorithm
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#
 *
 * @param child to estimate the cardinality of.
 * @param relativeSD the maximum estimation error allowed.
 */
// scalastyle:on
@ExpressionDescription(
  usage = """_FUNC_(expr) - Returns the estimated cardinality by HyperLogLog++.
    _FUNC_(expr, relativeSD=0.05) - Returns the estimated cardinality by HyperLogLog++
      with relativeSD, the maximum estimation error allowed.
    """)
case class HyperLogLogPlusPlus(
    child: Expression,
    relativeSD: Double = 0.05,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HLLPPDigest] {

  def this(child: Expression) = {
    this(child = child, relativeSD = 0.05, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)
  }

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def prettyName: String = "approx_count_distinct"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def createAggregationBuffer(): HLLPPDigest = {
    new HLLPPDigest(new HyperLogLogPlusPlusAlgo(relativeSD))
  }

  override def update(buffer: HLLPPDigest, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {
      buffer.hllpp.insert(v, child.dataType)
    }
  }

  override def merge(buffer: HLLPPDigest, input: HLLPPDigest): Unit = {
    buffer.hllpp.merge(input.hllpp)
  }

  override def eval(buffer: HLLPPDigest): Any = buffer.hllpp.query

  override def serialize(buffer: HLLPPDigest): Array[Byte] = {
    HyperLogLogPlusPlus.serializer.serialize(buffer)
  }

  override def deserialize(storageFormat: Array[Byte]): HLLPPDigest = {
    HyperLogLogPlusPlus.serializer.deserialize(storageFormat)
  }
}

object HyperLogLogPlusPlus {

  case class HLLPPDigest(hllpp: HyperLogLogPlusPlusAlgo)

  class HLLPPDigestSerializer {

    private final def length(hllpp: HyperLogLogPlusPlusAlgo): Int = {
      // hllpp.relativeSD, length of hllpp.words, hllpp.words
      Doubles.BYTES + Ints.BYTES + hllpp.words.length * Longs.BYTES
    }

    final def serialize(obj: HLLPPDigest): Array[Byte] = {
      val buffer = ByteBuffer.wrap(new Array(length(obj.hllpp)))
      buffer.putDouble(obj.hllpp.relativeSD)
      buffer.putInt(obj.hllpp.words.length)
      obj.hllpp.words.foreach(buffer.putLong)
      buffer.array()
    }

    final def deserialize(bytes: Array[Byte]): HLLPPDigest = {
      val buffer = ByteBuffer.wrap(bytes)
      val relativeSD = buffer.getDouble
      val wordsLength = buffer.getInt
      val words = new Array[Long](wordsLength)
      var i = 0
      while (i < wordsLength) {
        words(i) = buffer.getLong
        i += 1
      }
      new HLLPPDigest(new HyperLogLogPlusPlusAlgo(relativeSD, words))
    }
  }

  val serializer: HLLPPDigestSerializer = new HLLPPDigestSerializer

  private def validateDoubleLiteral(exp: Expression): Double = exp match {
    case Literal(d: Double, DoubleType) => d
    case Literal(dec: Decimal, _) => dec.toDouble
    case _ =>
      throw new AnalysisException("The second argument should be a double literal.")
  }
}
