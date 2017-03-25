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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ListQuery, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.types.BooleanType

class JoinOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Subquery", Once,
        OptimizeSubqueries) ::
      Batch("Filter Pushdown", FixedPoint(100),
        CombineFilters,
        PushDownPredicate,
        BooleanSimplification,
        ReorderJoin(SimpleCatalystConf(true)),
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) ::
      Batch("RewriteSubquery", Once,
        RewritePredicateSubquery) :: Nil

  }

  object OptimizeSubqueries extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case s: SubqueryExpression =>
        val Subquery(newPlan) = Optimize.execute(Subquery(s.plan))
        s.withNewPlan(newPlan)
    }
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int)

  test("extract filters and joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    def testExtract(plan: LogicalPlan, expected: Option[(Seq[LogicalPlan], Seq[Expression])]) {
      val expectedNoCross = expected map {
        seq_pair => {
          val plans = seq_pair._1
          val noCartesian = plans map { plan => (plan, Inner) }
          (noCartesian, seq_pair._2)
        }
      }
      testExtractCheckCross(plan, expectedNoCross)
    }

    def testExtractCheckCross
        (plan: LogicalPlan, expected: Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]) {
      assert(ExtractFiltersAndInnerJoins.unapply(plan) === expected)
    }

    testExtract(x, None)
    testExtract(x.where("x.b".attr === 1), None)
    testExtract(x.join(y), Some(Seq(x, y), Seq()))
    testExtract(x.join(y, condition = Some("x.b".attr === "y.d".attr)),
      Some(Seq(x, y), Seq("x.b".attr === "y.d".attr)))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr),
      Some(Seq(x, y), Seq("x.b".attr === "y.d".attr)))
    testExtract(x.join(y).join(z), Some(Seq(x, y, z), Seq()))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr).join(z),
      Some(Seq(x, y, z), Seq("x.b".attr === "y.d".attr)))
    testExtract(x.join(y).join(x.join(z)), Some(Seq(x, y, x.join(z)), Seq()))
    testExtract(x.join(y).join(x.join(z)).where("x.b".attr === "y.d".attr),
      Some(Seq(x, y, x.join(z)), Seq("x.b".attr === "y.d".attr)))

    testExtractCheckCross(x.join(y, Cross), Some(Seq((x, Cross), (y, Cross)), Seq()))
    testExtractCheckCross(x.join(y, Cross).join(z, Cross),
      Some(Seq((x, Cross), (y, Cross), (z, Cross)), Seq()))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some(Seq((x, Cross), (y, Cross), (z, Cross)), Seq("x.b".attr === "y.d".attr)))
    testExtractCheckCross(x.join(y, Inner, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some(Seq((x, Inner), (y, Inner), (z, Cross)), Seq("x.b".attr === "y.d".attr)))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Inner),
      Some(Seq((x, Cross), (y, Cross), (z, Inner)), Seq("x.b".attr === "y.d".attr)))
  }

  test("reorder inner joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    val queryAnswers = Seq(
      (
        x.join(y).join(z).where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, condition = Some("x.b".attr === "z.b".attr))
          .join(y, condition = Some("y.d".attr === "z.a".attr))
      ),
      (
        x.join(y, Cross).join(z, Cross)
          .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, Cross, Some("x.b".attr === "z.b".attr))
          .join(y, Cross, Some("y.d".attr === "z.a".attr))
      ),
      (
        x.join(y, Inner).join(z, Cross).where("x.b".attr === "z.a".attr),
        x.join(z, Cross, Some("x.b".attr === "z.a".attr)).join(y, Inner)
      )
    )

    queryAnswers foreach { queryAnswerPair =>
      val optimized = Optimize.execute(queryAnswerPair._1.analyze)
      comparePlans(optimized, analysis.EliminateSubqueryAliases(queryAnswerPair._2.analyze))
    }
  }

  test("reorder inner joins - don't put predicate with IN subquery into join condition") {
    // ReorderJoin collects all predicates and try to put them into join condition when creating
    // ordered join. If a predicate with an IN subquery is in a join condition instead of a filter
    // condition, `RewritePredicateSubquery.rewriteExistentialExpr` would fail to convert the
    // subquery to an ExistenceJoin, and thus result in error.
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)
    val w = testRelation1.subquery('w)
    val exists: AttributeReference = AttributeReference("exists", BooleanType, nullable = false)()

    val queryPlan = x.join(y).join(z)
      .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr) &&
        ("x.a".attr > 1 || "z.c".attr.in(ListQuery(w.select("w.d".attr)))))

    val expectedPlan = x.join(z, Inner, Some("x.b".attr === "z.b".attr))
      .join(w, ExistenceJoin(exists), Some("z.c".attr === "w.d".attr))
      .where("x.a".attr > 1 || exists)
      .select("x.a".attr, "x.b".attr, "x.c".attr, "z.a".attr, "z.b".attr, "z.c".attr)
      .join(y, Inner, Some("y.d".attr === "z.a".attr))

    val optimized = Optimize.execute(queryPlan.analyze)
    comparePlans(optimized, analysis.EliminateSubqueryAliases(expectedPlan.analyze))
  }

  test("broadcasthint sets relation statistics to smallest value") {
    val input = LocalRelation('key.int, 'value.string)

    val query =
      Project(Seq($"x.key", $"y.key"),
        Join(
          SubqueryAlias("x", input),
          BroadcastHint(SubqueryAlias("y", input)), Cross, None)).analyze

    val optimized = Optimize.execute(query)

    val expected =
      Join(
        Project(Seq($"x.key"), SubqueryAlias("x", input)),
        BroadcastHint(Project(Seq($"y.key"), SubqueryAlias("y", input))),
        Cross, None).analyze

    comparePlans(optimized, expected)

    val broadcastChildren = optimized.collect {
      case Join(_, r, _, _) if r.stats(conf).sizeInBytes == 1 => r
    }
    assert(broadcastChildren.size == 1)
  }
}
