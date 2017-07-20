/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.app.runtime.spark.sql.datasources.stream

import org.apache.spark.sql.sources._
import org.junit.{Assert, Test}

/**
  * Unit-test for [[StreamRelation]] functions without launch Spark SQL.
  */
class StreamRelationTest {

  /**
    * Test for simple filter for =, >, >=, <, <= operaters
    */
  @Test
  def testSimpleTimestampRange(): Unit = {
    // timestamp = x
    var ranges = StreamRelation.determineTimeRanges("ts", Array(EqualTo("ts", 10L)))
    Assert.assertEquals(Seq((10L, 11L)), ranges)

    // timestamp = x
    ranges = StreamRelation.determineTimeRanges("ts", Array(EqualNullSafe("ts", 10L)))
    Assert.assertEquals(Seq((10L, 11L)), ranges)

    // timestamp > x
    ranges = StreamRelation.determineTimeRanges("ts", Array(GreaterThan("ts", 10L)))
    Assert.assertEquals(Seq((11L, Long.MaxValue)), ranges)

    // timestamp >= x
    ranges = StreamRelation.determineTimeRanges("ts", Array(GreaterThanOrEqual("ts", 10L)))
    Assert.assertEquals(Seq((10L, Long.MaxValue)), ranges)

    // timestamp < x
    ranges = StreamRelation.determineTimeRanges("ts", Array(LessThan("ts", 10L)))
    Assert.assertEquals(Seq((0L, 10L)), ranges)

    // timestamp <= x
    ranges = StreamRelation.determineTimeRanges("ts", Array(LessThanOrEqual("ts", 10L)))
    Assert.assertEquals(Seq((0L, 11L)), ranges)
  }

  /**
    * Test for the IN filter, such as "WHERE ts in (1, 3, 5)"
    */
  @Test
  def testInFilter(): Unit = {
    // Distinct points, should have each of them a range
    var ranges = StreamRelation.determineTimeRanges("ts", Array(In("ts", Array(10L, 12L, 14L))))
    Assert.assertEquals(Seq((10L, 11L), (12L, 13L), (14L, 15L)), ranges);

    // Consective ranges should get collapsed
    ranges = StreamRelation.determineTimeRanges("ts", Array(In("ts", Array(10L, 11L, 12L, 14L, 15L, 18L))))
    Assert.assertEquals(Seq((10L, 13L), (14L, 16L), (18L, 19L)), ranges);
  }

  /**
    * Test for the OR filter, such as "WHERE ts < 10 OR ts > 20"
    */
  @Test
  def testOrFilter(): Unit = {
    // Simple OR
    var ranges = StreamRelation.determineTimeRanges("ts", Array(Or(LessThan("ts", 10L), GreaterThan("ts", 20L))))
    Assert.assertEquals(Seq((0L, 10L), (21L, Long.MaxValue)), ranges)

    // OR with IN
    ranges = StreamRelation.determineTimeRanges("ts", Array(Or(GreaterThan("ts", 30L), In("ts", Array(10L, 20L)))))
    Assert.assertEquals(Seq((10L, 11L), (20L, 21L), (31L, Long.MaxValue)), ranges)

    // OR with collapsing
    // (ts < 20 OR ts > 30) OR (ts < 15 OR ts > 25)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Or(Or(LessThan("ts", 20L), GreaterThan("ts", 30L)),
                                                                    Or(LessThan("ts", 15L), GreaterThan("ts", 25L)))))
    Assert.assertEquals(Seq((0L, 20L), (26L, Long.MaxValue)), ranges)

    // OR with a non timestamp filter. Expect that to cause full scan.
    // (ts < 20 OR x = y)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Or(LessThan("ts", 20L), EqualTo("x", "y"))))
    Assert.assertEquals(Seq((0L, Long.MaxValue)), ranges)
  }

  /**
    * Test for the AND filter, such as "WHERE ts > 10 AND ts < 20"
    */
  @Test
  def testAndFilter(): Unit = {
    // Simple AND (using top level filters, which Spark treats ANDing all of them)
    var ranges = StreamRelation.determineTimeRanges("ts", Array(GreaterThan("ts", 10L), LessThan("ts", 20L)))
    Assert.assertEquals(Seq((11L, 20L)), ranges)

    // AND using the AND filter explicitly
    ranges = StreamRelation.determineTimeRanges("ts", Array(And(GreaterThan("ts", 10L), LessThan("ts", 20L))))
    Assert.assertEquals(Seq((11L, 20L)), ranges)

    // AND with a non timestamp filter
    // (ts > 10 AND ts < 20 AND x = y)
    ranges = StreamRelation.determineTimeRanges("ts", Array(GreaterThan("ts", 10L),
                                                                 LessThan("ts", 20L),
                                                                 EqualTo("x", "y")))
    Assert.assertEquals(Seq((11L, 20L)), ranges)

    // AND with condition that can never be satisfied.
    // (ts < 10 AND ts > 20)
    ranges = StreamRelation.determineTimeRanges("ts", Array(LessThan("ts", 10L), GreaterThan("ts", 20L)))
    Assert.assertTrue(ranges.isEmpty)

    // OR + AND
    // (ts > 10 AND ts < 20) OR (ts > 40 AND ts < 50)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Or(And(GreaterThan("ts", 10L), LessThan("ts", 20L)),
                                                                    And(GreaterThan("ts", 40L), LessThan("ts", 50L)))))
    Assert.assertEquals(Seq((11L, 20L), (41L, 50L)), ranges)

    // AND + OR
    // (ts < 10 OR ts > 20) AND (ts = 40 OR ts > 100)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Or(LessThan("ts", 10L), GreaterThan("ts", 20L)),
                                                                 Or(EqualTo("ts", 40L), GreaterThan("ts", 100L))))
    Assert.assertEquals(Seq((40L, 41L), (101L, Long.MaxValue)), ranges)
  }

  @Test
  def testNotFilter(): Unit = {
    // For NOT = and NOT IN, we simply use the whole time range (see explanations in the implementation)
    var ranges = StreamRelation.determineTimeRanges("ts", Array(Not(EqualTo("ts", 20L))))
    Assert.assertEquals(Seq((0L, Long.MaxValue)), ranges)

    // NOT IN
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(In("ts", Array(20L, 30L)))))
    Assert.assertEquals(Seq((0L, Long.MaxValue)), ranges)

    // !(ts > 20)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(GreaterThan("ts", 20L))))
    Assert.assertEquals(Seq((0, 21L)), ranges)

    // !(ts >= 20)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(GreaterThanOrEqual("ts", 20L))))
    Assert.assertEquals(Seq((0, 20L)), ranges)

    // !(ts < 20)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(LessThan("ts", 20L))))
    Assert.assertEquals(Seq((20L, Long.MaxValue)), ranges)

    // !(ts <= 20)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(LessThanOrEqual("ts", 20L))))
    Assert.assertEquals(Seq((21L, Long.MaxValue)), ranges)

    // !(ts < 20 OR ts > 30)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(Or(LessThan("ts", 20L), GreaterThan("ts", 30L)))))
    Assert.assertEquals(Seq((20L, 31L)), ranges)

    // !(ts < 30) OR !(ts > 20)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Or(Not(LessThan("ts", 30L)),
                                                                    Not(GreaterThan("ts", 20L)))))
    Assert.assertEquals(Seq((0, 21), (30, Long.MaxValue)), ranges)

    // !(ts > 20 AND ts < 30)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(And(GreaterThan("ts", 20L),
                                                                        (LessThan("ts", 30L))))))
    Assert.assertEquals(Seq((0, 21), (30, Long.MaxValue)), ranges)

    // NOT NOT (double negative)
    ranges = StreamRelation.determineTimeRanges("ts", Array(Not(Not(GreaterThan("ts", 20L)))))
    Assert.assertEquals(Seq((21L, Long.MaxValue)), ranges)
  }
}
