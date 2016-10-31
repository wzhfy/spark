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

package org.apache.spark.sql.execution.command

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{CreateArray, _}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._


/**
 * Analyzes the given columns of the given table to generate statistics, which will be used in
 * query optimizations.
 */
case class AnalyzeColumnCommand(
    tableIdent: TableIdentifier,
    columnNames: Seq[String]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val relation = EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdentWithDB))

    relation match {
      case catalogRel: CatalogRelation =>
        updateStats(catalogRel.catalogTable,
          AnalyzeTableCommand.calculateTotalSize(sessionState, catalogRel.catalogTable))

      case logicalRel: LogicalRelation if logicalRel.catalogTable.isDefined =>
        updateStats(logicalRel.catalogTable.get, logicalRel.relation.sizeInBytes)

      case otherRelation =>
        throw new AnalysisException("ANALYZE TABLE is not supported for " +
          s"${otherRelation.nodeName}.")
    }

    def updateStats(catalogTable: CatalogTable, newTotalSize: Long): Unit = {
      val (rowCount, columnStats) = computeColStats(sparkSession, relation)
      // We also update table-level stats in order to keep them consistent with column-level stats.
      val statistics = Statistics(
        sizeInBytes = newTotalSize,
        rowCount = Some(rowCount),
        // Newly computed column stats should override the existing ones.
        colStats = catalogTable.stats.map(_.colStats).getOrElse(Map()) ++ columnStats)
      sessionState.catalog.alterTable(catalogTable.copy(stats = Some(statistics)))
      // Refresh the cached data source table in the catalog.
      sessionState.catalog.refreshTable(tableIdentWithDB)
    }

    Seq.empty[Row]
  }

  def computeColStats(
      sparkSession: SparkSession,
      relation: LogicalPlan): (Long, Map[String, LeafColumnStat]) = {

    // check correctness of column names
    val attributesToAnalyze = mutable.MutableList[Attribute]()
    val duplicatedColumns = mutable.MutableList[String]()
    val resolver = sparkSession.sessionState.conf.resolver
    columnNames.foreach { col =>
      val exprOption = relation.output.find(attr => resolver(attr.name, col))
      val expr = exprOption.getOrElse(throw new AnalysisException(s"Invalid column name: $col."))
      // do deduplication
      if (!attributesToAnalyze.contains(expr)) {
        attributesToAnalyze += expr
      } else {
        duplicatedColumns += col
      }
    }
    if (duplicatedColumns.nonEmpty) {
      logWarning("Duplicate column names were deduplicated in `ANALYZE TABLE` statement. " +
        s"Input columns: ${columnNames.mkString("(", ", ", ")")}. " +
        s"Duplicate columns: ${duplicatedColumns.mkString("(", ", ", ")")}.")
    }

    // Collect statistics per column.
    // The first element in the result will be the overall row count, the following elements
    // will be structs containing all column stats.
    // The layout of each struct follows the layout of the ColumnStats.
    val sqlConf = sparkSession.sessionState.conf
    val expressions = Count(Literal(1)).toAggregateExpression() +: attributesToAnalyze.map(
      ColumnStatStruct(_,
        relativeSD = sqlConf.ndvMaxError,
        numBins = sqlConf.histogramMaxBins,
        accuracy = sqlConf.percentileAccuracy))
    val namedExpressions = expressions.map(e => Alias(e, e.toString)())
    val statsRow = Dataset.ofRows(sparkSession, Aggregate(Nil, namedExpressions, relation))
      .queryExecution.toRdd.collect().head

    // unwrap the result
    val rowCount = statsRow.getLong(0)
    // Aggs for computing equi-height histograms
    val intervalDistinctAggs = new ArrayBuffer[(Attribute, Expression)]()
    val columnStats = attributesToAnalyze.zipWithIndex.map { case (attr, i) =>
      val numFields = ColumnStatStruct.numStatFields(attr.dataType)
      val structRow = statsRow.getStruct(i + 1, numFields)
      // check if we need to compute equi-height histograms
      if (attr.dataType.isInstanceOf[NumericType] || attr.dataType.isInstanceOf[DateType] ||
        attr.dataType.isInstanceOf[TimestampType]) {
        // get the last result, which is endpoints of histogram
        val mapData = structRow.getMap(4)
        if (mapData.numElements() > sqlConf.histogramMaxBins) {
          // the result contains `numBins+1` endpoints, which means we need to compute an
          // equi-height histogram for this column
          intervalDistinctAggs += ((attr, new IntervalDistinctApprox(attr,
            CreateArray(mapData.keyArray().toDoubleArray().map(Literal(_))), sqlConf.ndvMaxError)))
        }
      }
      (attr.name, LeafColumnStat(structRow))
    }.toMap

    val statsWithIntervalDistinct = new mutable.HashMap[String, LeafColumnStat]()
    if (intervalDistinctAggs.nonEmpty) {
      val namedIntervalDistinctAggs = expressions.map(e => Alias(e, e.toString)())
      val intervalDistinctRow = Dataset.ofRows(sparkSession,
        Aggregate(Nil, namedIntervalDistinctAggs, relation)).queryExecution.toRdd.collect().head
      // concatenate the previous statRow with this row
      intervalDistinctAggs.zipWithIndex.map { case ((attr, agg), i) =>
        val structRow = intervalDistinctRow.getStruct(i, 1)
        val schema1 = ColumnStatStruct.numericColumnStat(
          e = attr,
          relativeSD = sqlConf.ndvMaxError,
          numBins = sqlConf.histogramMaxBins,
          accuracy = sqlConf.percentileAccuracy)
          .map(e => Alias(e, e.toString)().toAttribute).toStructType
        val schema2 = Seq(Alias(agg, agg.toString)().toAttribute).toStructType
        val combiner = GenerateUnsafeRowJoiner.create(schema1, schema2)
        val joinedRow = combiner.join(columnStats(attr.name).statRow.asInstanceOf[UnsafeRow],
          structRow.asInstanceOf[UnsafeRow])
        statsWithIntervalDistinct.put(attr.name, LeafColumnStat(joinedRow))
      }
    }
    // update the previous statRow with the joined row
    (rowCount, columnStats ++ statsWithIntervalDistinct)
  }
}

object ColumnStatStruct {
  private val zero = Literal(0, LongType)
  private val one = Literal(1, LongType)

  private def numNulls(e: Expression): Expression = {
    if (e.nullable) Sum(If(IsNull(e), one, zero)) else zero
  }
  private def max(e: Expression): Expression = Max(e)
  private def min(e: Expression): Expression = Min(e)
  private def ndv(e: Expression, relativeSD: Double): Expression = {
    // the approximate ndv should never be larger than the number of rows
    Least(Seq(HyperLogLogPlusPlus(e, relativeSD), Count(one)))
  }
  private def map(e: Expression, numBins: Int): Expression = new MapAggregate(e, Literal(numBins))
  private def percentile(e: Expression, numBins: Int, accuracy: Int): Expression = {
    val percentages = (0 to numBins).map(p => Literal(p / numBins.toDouble))
    new ApproximatePercentile(e, CreateArray(percentages), Literal(accuracy))
  }
  private def countIntervals(e: Expression, relativeSD: Double, numBins: Int): Expression = {
    new IntervalDistinctApprox(e, Literal(numBins), relativeSD)
  }
  private def avgLength(e: Expression): Expression = Average(Length(e))
  private def maxLength(e: Expression): Expression = Max(Length(e))
  private def numTrues(e: Expression): Expression = Sum(If(e, one, zero))
  private def numFalses(e: Expression): Expression = Sum(If(Not(e), one, zero))

  private def getStruct(exprs: Seq[Expression]): CreateStruct = {
    CreateStruct(exprs.map { expr: Expression =>
      expr.transformUp {
        case af: AggregateFunction => af.toAggregateExpression()
      }
    })
  }

  def numericColumnStat(
      e: Expression,
      relativeSD: Double,
      numBins: Int,
      accuracy: Int): Seq[Expression] = {
    Seq(numNulls(e), max(e), min(e), ndv(e, relativeSD), map(e, numBins),
      percentile(e, numBins = numBins, accuracy = accuracy))
  }

  private def stringColumnStat(e: Expression, relativeSD: Double, numBins: Int): Seq[Expression] = {
    Seq(numNulls(e), avgLength(e), maxLength(e), ndv(e, relativeSD), map(e, numBins))
  }

  private def binaryColumnStat(e: Expression): Seq[Expression] = {
    Seq(numNulls(e), avgLength(e), maxLength(e))
  }

  private def booleanColumnStat(e: Expression): Seq[Expression] = {
    Seq(numNulls(e), numTrues(e), numFalses(e))
  }

  def numStatFields(dataType: DataType): Int = {
    dataType match {
      case BinaryType | BooleanType => 3
      case StringType => 5
      case _ => 6
    }
  }

  def apply(
      attr: Attribute,
      relativeSD: Double,
      numBins: Int,
      accuracy: Int): CreateStruct = attr.dataType match {
    // Use aggregate functions to compute statistics we need.
    case _: NumericType | TimestampType | DateType =>
      getStruct(numericColumnStat(
        attr, relativeSD = relativeSD, numBins = numBins, accuracy = accuracy))
    case StringType => getStruct(stringColumnStat(attr, relativeSD = relativeSD, numBins = numBins))
    case BinaryType => getStruct(binaryColumnStat(attr))
    case BooleanType => getStruct(booleanColumnStat(attr))
    case otherType =>
      throw new AnalysisException("Analyzing columns is not supported for column " +
        s"${attr.name} of data type: ${attr.dataType}.")
  }
}
