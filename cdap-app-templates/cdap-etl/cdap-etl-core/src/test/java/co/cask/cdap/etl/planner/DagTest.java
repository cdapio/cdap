/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.planner;

import co.cask.cdap.etl.proto.Connection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class DagTest {

  @Test
  public void testTopologicalOrder() {
    // n1 -> n2 -> n3 -> n4
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"), new Connection("n2", "n3"), new Connection("n3", "n4")));
    Assert.assertEquals(ImmutableList.of("n1", "n2", "n3", "n4"), dag.getTopologicalOrder());

    /*
             |--- n2 ---|
        n1 --|          |-- n4
             |--- n3 ---|
     */
    dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4")));
    // could be n1 -> n2 -> n3 -> n4
    // or it could be n1 -> n3 -> n2 -> n4
    List<String> linearized = dag.getTopologicalOrder();
    Assert.assertEquals("n1", linearized.get(0));
    Assert.assertEquals("n4", linearized.get(3));
    assertBefore(linearized, "n1", "n2");
    assertBefore(linearized, "n1", "n3");

    /*
        n1 --|
             |--- n3
        n2 --|
     */
    dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n3"),
      new Connection("n2", "n3")));
    // could be n1 -> n2 -> n3
    // or it could be n2 -> n1 -> n3
    linearized = dag.getTopologicalOrder();
    Assert.assertEquals("n3", linearized.get(2));
    assertBefore(linearized, "n1", "n3");
    assertBefore(linearized, "n2", "n3");

    /*
                                     |--- n3
             |--- n2 ----------------|
        n1 --|       |               |--- n5
             |--------- n4 ----------|
             |              |        |
             |---------------- n6 ---|

        vertical arrows are pointing down
     */
    dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n4"),
      new Connection("n1", "n6"),
      new Connection("n2", "n3"),
      new Connection("n2", "n4"),
      new Connection("n2", "n5"),
      new Connection("n4", "n3"),
      new Connection("n4", "n5"),
      new Connection("n4", "n6"),
      new Connection("n6", "n3"),
      new Connection("n6", "n5")));
    linearized = dag.getTopologicalOrder();
    Assert.assertEquals("n1", linearized.get(0));
    Assert.assertEquals("n2", linearized.get(1));
    Assert.assertEquals("n4", linearized.get(2));
    Assert.assertEquals("n6", linearized.get(3));
    assertBefore(linearized, "n6", "n3");
    assertBefore(linearized, "n6", "n5");
  }

  private void assertBefore(List<String> list, String a, String b) {
    int aIndex = list.indexOf(a);
    int bIndex = list.indexOf(b);
    Assert.assertTrue(aIndex < bIndex);
  }

  @Test(expected = IllegalStateException.class)
  public void testCycle() {
    /*
             |---> n2 ----|
             |      ^     v
        n1 --|      |     n3 --> n5
             |---> n4 <---|
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n2"),
      new Connection("n3", "n5")));
    dag.getTopologicalOrder();
  }

  @Test
  public void testRemoveSource() {
    /*
             |--- n2 ---|
        n1 --|          |-- n4
             |--- n3 ---|
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4")));
    Assert.assertEquals("n1", dag.removeSource());
    Assert.assertEquals(ImmutableSet.of("n2", "n3"), dag.getSources());

    Set<String> removed = ImmutableSet.of(dag.removeSource(), dag.removeSource());
    Assert.assertEquals(ImmutableSet.of("n2", "n3"), removed);
    Assert.assertEquals(ImmutableSet.of("n4"), dag.getSources());
    Assert.assertEquals("n4", dag.removeSource());
    Assert.assertTrue(dag.getSources().isEmpty());
    Assert.assertTrue(dag.getSinks().isEmpty());
    Assert.assertNull(dag.removeSource());
  }

  @Test
  public void testIslands() {
    /*
        n1 -- n2

        n3 -- n4
     */
    try {
      new Dag(ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n3", "n4")));
      Assert.fail();
    } catch (DisjointConnectionsException e) {
      // expected
      Assert.assertTrue(e.getMessage().startsWith("Invalid DAG. There is an island"));
    }

    /*
        n1 -- n2
              |
              v
        n3 -- n4
              ^
        n5----|   n6 -- n7
     */
    try {
      new Dag(ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n4"),
        new Connection("n3", "n4"),
        new Connection("n5", "n4"),
        new Connection("n6", "n7")));
      Assert.fail();
    } catch (DisjointConnectionsException e) {
      // expected
      Assert.assertTrue(e.getMessage().startsWith("Invalid DAG. There is an island"));
    }
  }

  @Test
  public void testAccessibleFrom() {
    /*
        n1 -- n2
              |
              v
        n3 -- n4 --- n8
              ^
              |
        n5-------- n6 -- n7
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4"),
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n4", "n8"), dag.accessibleFrom("n1"));
    Assert.assertEquals(ImmutableSet.of("n2", "n4", "n8"), dag.accessibleFrom("n2"));
    Assert.assertEquals(ImmutableSet.of("n3", "n4", "n8"), dag.accessibleFrom("n3"));
    Assert.assertEquals(ImmutableSet.of("n4", "n8"), dag.accessibleFrom("n4"));
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n8", "n6", "n7"), dag.accessibleFrom("n5"));
    Assert.assertEquals(ImmutableSet.of("n6", "n7"), dag.accessibleFrom("n6"));
    Assert.assertEquals(ImmutableSet.of("n7"), dag.accessibleFrom("n7"));
    Assert.assertEquals(ImmutableSet.of("n8"), dag.accessibleFrom("n8"));

    // this stop node isn't accessible from n5 anyway, shouldn't have any effect
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n8", "n6", "n7"),
                        dag.accessibleFrom("n5", ImmutableSet.of("n1")));
    // these stop nodes cut off all paths, though the stop nodes themselves should show up in accessible set
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n6"), dag.accessibleFrom("n5", ImmutableSet.of("n4", "n6")));
    // these stop nodes cut off some paths
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n6", "n7"),
                        dag.accessibleFrom("n5", ImmutableSet.of("n4")));
    // these stop nodes cut off some paths
    Assert.assertEquals(ImmutableSet.of("n5", "n4", "n6", "n8"),
                        dag.accessibleFrom("n5", ImmutableSet.of("n6", "n1")));
  }

  @Test
  public void testParentsOf() {
    /*
        n1 -> n2
              |
              v
        n3 -> n4 --> n8
              ^
              |
        n5-------> n6 -> n7
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4"),
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));
    Assert.assertEquals(ImmutableSet.of("n1"), dag.parentsOf("n1"));
    Assert.assertEquals(ImmutableSet.of("n1", "n2"), dag.parentsOf("n2"));
    Assert.assertEquals(ImmutableSet.of("n3"), dag.parentsOf("n3"));
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n3", "n4", "n5"), dag.parentsOf("n4"));
    Assert.assertEquals(ImmutableSet.of("n5"), dag.parentsOf("n5"));
    Assert.assertEquals(ImmutableSet.of("n5", "n6"), dag.parentsOf("n6"));
    Assert.assertEquals(ImmutableSet.of("n5", "n6", "n7"), dag.parentsOf("n7"));
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n3", "n4", "n5", "n8"), dag.parentsOf("n8"));

    // these stop nodes are not parents, shouldn't have any effect
    Assert.assertEquals(ImmutableSet.of("n1", "n2", "n3", "n4", "n5", "n8"),
                        dag.parentsOf("n8", ImmutableSet.of("n6", "n7")));
    Assert.assertEquals(ImmutableSet.of("n5", "n6", "n7"),
                        dag.parentsOf("n7", ImmutableSet.of("n4")));
    // these stop nodes cut off all paths except itself
    Assert.assertEquals(ImmutableSet.of("n6", "n7"), dag.parentsOf("n7", ImmutableSet.of("n5", "n6")));
    // these stop nodes cut off some paths
    Assert.assertEquals(ImmutableSet.of("n2", "n3", "n5", "n4", "n8"),
                        dag.parentsOf("n8", ImmutableSet.of("n2", "n3")));
    Assert.assertEquals(ImmutableSet.of("n2", "n3", "n4", "n5"),
                        dag.parentsOf("n4", ImmutableSet.of("n2", "n3")));
  }

  @Test
  public void testSubsetAround() {
    /*
         n1 --> n2 --|
                     |--> n3 --> n4 --|
         n7 --> n8 --|                |--> n5 --> n6
                                      |
         n9 --------------------------|
     */
    Dag fullDag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n7", "n8"),
      new Connection("n8", "n3"),
      new Connection("n9", "n5")));

    // test without stop nodes

    /*
         n1 --> n2 --|
                     |--> n3 --> n4 --|
                                      |--> n5 --> n6
     */
    Dag expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6")));
    Assert.assertEquals(expected, fullDag.subsetAround("n2", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(expected, fullDag.subsetAround("n1", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    /*
         n1 --> n2 --|
                     |--> n3 --> n4 --|
         n7 --> n8 --|                |--> n5 --> n6
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n7", "n8"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n3", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(expected, fullDag.subsetAround("n4", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    Assert.assertEquals(fullDag, fullDag.subsetAround("n5", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(fullDag, fullDag.subsetAround("n6", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    /*
                     |--> n3 --> n4 --|
         n7 --> n8 --|                |--> n5 --> n6
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n7", "n8"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n7", ImmutableSet.<String>of(), ImmutableSet.<String>of()));
    Assert.assertEquals(expected, fullDag.subsetAround("n8", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    /*
                                      |--> n5 --> n6
                                      |
         n9 --------------------------|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n5", "n6"),
      new Connection("n9", "n5")));
    Assert.assertEquals(expected, fullDag.subsetAround("n9", ImmutableSet.<String>of(), ImmutableSet.<String>of()));

    // test with stop nodes

    /*
         n1 --> n2 --|
                     |--> n3 --> n4
                n8 --|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n3", ImmutableSet.of("n4"), ImmutableSet.of("n1", "n8")));

    /*
         n1 --> n2 --|
                     |--> n3 --> n4
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n3"),
      new Connection("n3", "n4")));
    Assert.assertEquals(expected, fullDag.subsetAround("n2", ImmutableSet.of("n4"), ImmutableSet.of("n1", "n8")));
    Assert.assertEquals(expected, fullDag.subsetAround("n1", ImmutableSet.of("n4"), ImmutableSet.of("n1", "n8")));

    /*
                          n3 --> n4 --|
                                      |--> n5 --> n6
                                      |
         n9 --------------------------|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n9", "n5")));
    Assert.assertEquals(expected, fullDag.subsetAround("n5", ImmutableSet.of("n6"), ImmutableSet.of("n3", "n9")));
    Assert.assertEquals(expected, fullDag.subsetAround("n6", ImmutableSet.of("n6"), ImmutableSet.of("n3", "n9")));

    /*
                n2 --|
                     |--> n3 --> n4 --|
                n8 --|                |--> n5
                                      |
         n9 --------------------------|
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n5", "n6"),
      new Connection("n8", "n3"),
      new Connection("n9", "n5")));
    Assert.assertEquals(expected, fullDag.subsetAround("n5", ImmutableSet.of("n6"), ImmutableSet.of("n2", "n8")));

    /*
                n2 --|
                     |--> n3 --> n4 --|
                n8 --|                |--> n5
     */
    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "n5"),
      new Connection("n8", "n3")));
    Assert.assertEquals(expected, fullDag.subsetAround("n4", ImmutableSet.of("n5"), ImmutableSet.of("n2", "n8")));
  }

  @Test
  public void testSubset() {
    /*
        n1 -- n2
              |
              v
        n3 -- n4 --- n8
              ^
              |
        n5-------- n6 -- n7
     */
    Dag fulldag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n3", "n4"),
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));

    Dag expected = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n2", "n4"),
      new Connection("n4", "n8")));
    Dag actual = fulldag.subsetFrom("n1");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n4"),
      new Connection("n4", "n8")));
    actual = fulldag.subsetFrom("n2");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n4"),
      new Connection("n4", "n8")));
    actual = fulldag.subsetFrom("n3");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n4", "n8"),
      new Connection("n5", "n4"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7")));
    actual = fulldag.subsetFrom("n5");
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n6", "n7")));
    actual = fulldag.subsetFrom("n6");
    Assert.assertEquals(expected, actual);

    // test subsets with stop nodes
    expected = new Dag(ImmutableSet.of(new Connection("n1", "n2")));
    actual = fulldag.subsetFrom("n1", ImmutableSet.of("n2"));
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n5", "n4"),
      new Connection("n5", "n6")));
    actual = fulldag.subsetFrom("n5", ImmutableSet.of("n4", "n6"));
    Assert.assertEquals(expected, actual);


    /*
             |--- n2 ----------|
             |                 |                              |-- n10
        n1 --|--- n3 --- n5 ---|--- n6 --- n7 --- n8 --- n9 --|
             |                 |                              |-- n11
             |--- n4 ----------|

     */
    fulldag = new Dag(ImmutableSet.of(
      new Connection("n1", "n2"),
      new Connection("n1", "n3"),
      new Connection("n1", "n4"),
      new Connection("n2", "n6"),
      new Connection("n3", "n5"),
      new Connection("n4", "n6"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7"),
      new Connection("n7", "n8"),
      new Connection("n8", "n9"),
      new Connection("n9", "n10"),
      new Connection("n9", "n11")));

    expected = new Dag(ImmutableSet.of(
      new Connection("n3", "n5"),
      new Connection("n5", "n6"),
      new Connection("n6", "n7"),
      new Connection("n7", "n8"),
      new Connection("n8", "n9")));
    actual = fulldag.subsetFrom("n3", ImmutableSet.of("n4", "n9"));
    Assert.assertEquals(expected, actual);

    expected = new Dag(ImmutableSet.of(
      new Connection("n2", "n6"),
      new Connection("n6", "n7"),
      new Connection("n7", "n8")));
    actual = fulldag.subsetFrom("n2", ImmutableSet.of("n4", "n8", "n1"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSplitByControlNodes() throws Exception {

    // In following test cases note that Action nodes are named as (a0, a1...) and condition nodes are named
    // as (c0, c1, ..)


    // Test condition in the beginning and one branch connects to the action.

    // c1 --> a1 --> n1 --> n2
    //  |
    //  | --> n3 --> n4 --> a2

    Dag dag = new Dag(ImmutableSet.of(
      new Connection("c1", "a1"),
      new Connection("a1", "n1"),
      new Connection("n1", "n2"),
      new Connection("c1", "n3"),
      new Connection("n3", "n4"),
      new Connection("n4", "a2")));

    Set<Dag> actual = dag.splitByControlNodes(new HashSet<>(Arrays.asList("c1", "a1", "a2")));
    Set<Dag> expectedDags = new HashSet<>();
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "a1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "n3"),
                                             new Connection("n3", "n4"),
                                             new Connection("n4", "a2"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a1", "n1"), new Connection("n1", "n2"))));

    Assert.assertEquals(expectedDags, actual);

    // Test condition in the end and branches connects to the Action.
    // n0-->n1--c0-->n2-->c1-->a1
    //                    |
    //                    |-->a2
    dag = new Dag(ImmutableSet.of(
      new Connection("n0", "n1"),
      new Connection("n1", "c0"),
      new Connection("c0", "n2"),
      new Connection("n2", "c1"),
      new Connection("c1", "a1"),
      new Connection("c1", "a2")));

    actual = dag.splitByControlNodes(new HashSet<>(Arrays.asList("c0", "c1", "a1", "a2")));
    expectedDags.clear();
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("n0", "n1"),
                                             new Connection("n1", "c0"))));

    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c0", "n2"),
                                             new Connection("n2", "c1"))));

    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "a2"))));

    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "a1"))));

    Assert.assertEquals(expectedDags, actual);

    // Test Actions in the beginning and connects to the Condition.
    // a1 - a2 - c1 - n0
    //      |
    // a0 --

    dag = new Dag(ImmutableSet.of(
      new Connection("a0", "a2"),
      new Connection("a1", "a2"),
      new Connection("a2", "c1"),
      new Connection("c1", "n0")));

    actual = dag.splitByControlNodes(new HashSet<>(Arrays.asList("a0", "a1", "a2", "c1")));
    expectedDags.clear();
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a0", "a2"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a1", "a2"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a2", "c1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "n0"))));
    Assert.assertEquals(expectedDags, actual);

    // Tests Actions in the beginning and connect to the Condition through other plugin
    // a1 - n0 - c1 - n1
    //      |
    // a0 --

    dag = new Dag(ImmutableSet.of(
      new Connection("a0", "n0"),
      new Connection("a1", "n0"),
      new Connection("n0", "c1"),
      new Connection("c1", "n1")));

    actual = dag.splitByControlNodes(new HashSet<>(Arrays.asList("a0", "a1", "c1")));

    expectedDags.clear();
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a0", "n0"),
                                             new Connection("n0", "c1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("a1", "n0"),
                                             new Connection("n0", "c1"))));
    expectedDags.add(new Dag(ImmutableSet.of(new Connection("c1", "n1"))));
    Assert.assertEquals(expectedDags, actual);
  }
}
