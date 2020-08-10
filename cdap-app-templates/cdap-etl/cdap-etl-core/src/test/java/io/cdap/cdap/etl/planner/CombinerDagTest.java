/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.planner;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.etl.proto.Connection;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Tests for {@link CombinerDag}
 */
public class CombinerDagTest {

  @Test
  public void testGroupTwoBranches() {
    /*
            |--> k1
        s --|
            |--> k2
     */
    Dag dag = new Dag(ImmutableSet.of(new Connection("s", "k1"), new Connection("s", "k2")));
    CombinerDag combinerDag = new CombinerDag(dag, Collections.emptySet(), new GroupIdGenerator());

    Map<String, Set<String>> groups = combinerDag.groupNodes();
    Assert.assertEquals(1, groups.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("k1", "k2")), groups.values().iterator().next());

    Dag expected = new Dag(Collections.singleton(new Connection("s", "group0")));
    Dag actual = new Dag(combinerDag);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testTwoBranchesShouldNotGroup() {
    /*
            |--> a1 --> k1
        s --|
            |--> k2
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("s", "a1"),
      new Connection("a1", "k1"),
      new Connection("s", "k2")));
    CombinerDag combinerDag = new CombinerDag(dag, Collections.singleton("a1"));

    Assert.assertTrue(combinerDag.groupNodes().isEmpty());
    Assert.assertEquals(dag, new Dag(combinerDag));
  }

  @Test
  public void testUncombinableSink() {
    /*
            |--> k1
        s --|
            |--> k2
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("s", "k1"),
      new Connection("s", "k2")));
    CombinerDag combinerDag = new CombinerDag(dag, Collections.singleton("k1"));

    Assert.assertTrue(combinerDag.groupNodes().isEmpty());
    Assert.assertEquals(dag, new Dag(combinerDag));
  }

  @Test
  public void testSomeBranchesGroup() {
    /*
            |--> a1 --> k1
            |
        s --|--> k2
            |
            |--> k3
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("s", "a1"),
      new Connection("a1", "k1"),
      new Connection("s", "k2"),
      new Connection("s", "k3")));
    CombinerDag combinerDag = new CombinerDag(dag, Collections.singleton("a1"), new GroupIdGenerator());

    Map<String, Set<String>> groups = combinerDag.groupNodes();
    Assert.assertEquals(1, groups.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("k2", "k3")), groups.values().iterator().next());

    Dag expected = new Dag(ImmutableSet.of(
      new Connection("s", "a1"),
      new Connection("a1", "k1"),
      new Connection("s", "group0")));
    Assert.assertEquals(expected, new Dag(combinerDag));
  }

  @Test
  public void testOverlappingGroups() {
    /*
             |--> k1
        s1 --|
             |--> k2
                  ^
           s2 ----|
                  |--> k3
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("s1", "k1"),
      new Connection("s1", "k2"),
      new Connection("s2", "k2"),
      new Connection("s2", "k3")));
    CombinerDag combinerDag = new CombinerDag(dag, Collections.emptySet(), new GroupIdGenerator());

    Map<String, Set<String>> groups = combinerDag.groupNodes();
    Assert.assertEquals(1, groups.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("k1", "k2", "k3")), groups.values().iterator().next());

    Dag expected = new Dag(ImmutableSet.of(
      new Connection("s1", "group1"),
      new Connection("s2", "group1")));
    Assert.assertEquals(expected, new Dag(combinerDag));
  }

  @Test
  public void testCandidateOverlaps() {
    /*
            |--> a1 --> k1
            |           ^
        s --|--> t1 ----|
            |           |--> k2
            |--> k3
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("s", "a1"),
      new Connection("a1", "k1"),
      new Connection("s", "t1"),
      new Connection("t1", "k1"),
      new Connection("t1", "k2"),
      new Connection("s", "k3")));
    CombinerDag combinerDag = new CombinerDag(dag, Collections.singleton("a1"), new GroupIdGenerator());

    Map<String, Set<String>> groups = combinerDag.groupNodes();
    Assert.assertEquals(1, groups.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("k1", "k2")), groups.values().iterator().next());

    Dag expected = new Dag(ImmutableSet.of(
      new Connection("s", "a1"),
      new Connection("a1", "group0"),
      new Connection("s", "t1"),
      new Connection("t1", "group0"),
      new Connection("s", "k3")));
    Assert.assertEquals(expected, new Dag(combinerDag));
  }

  @Test
  public void testCandidateOverlapsWithUncombinable() {
    /*
            |--------- a1 --> k1
            |           ^
        s --|--> t1 ----|
            |           |--> k2
            |--> k3
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("s", "a1"),
      new Connection("a1", "k1"),
      new Connection("s", "t1"),
      new Connection("t1", "a1"),
      new Connection("t1", "k2"),
      new Connection("s", "k3")));
    CombinerDag combinerDag = new CombinerDag(dag, Collections.singleton("a1"));
    Map<String, Set<String>> groups = combinerDag.groupNodes();

    Assert.assertTrue(groups.isEmpty());
    Assert.assertEquals(dag, new Dag(combinerDag));
  }

  @Test
  public void testComplex() {
    /*

                                 |--> K1
                       |--> T2 --|
             |--> T1 --|         |--> K2
             |         |--> K3
             |
         S --|--> T3 --|
             |         |--|
             |            |--> K4
             |         |--|
             |--> T4 --|                |--> A2 --> T6 --|
                       |                |                |--> K5
                       |                |--> T7 ---------|
                       |--> A1 --> T5 --|
                                        |--> T8 --> K6
                                        |
                                        |--> T9 --> K7
     */
    Dag dag = new Dag(ImmutableSet.of(
      new Connection("s", "t1"),
      new Connection("s", "t3"),
      new Connection("s", "t4"),
      new Connection("t1", "t2"),
      new Connection("t1", "k3"),
      new Connection("t2", "k1"),
      new Connection("t2", "k2"),
      new Connection("t3", "k4"),
      new Connection("t4", "k4"),
      new Connection("t4", "a1"),
      new Connection("a1", "t5"),
      new Connection("t5", "a2"),
      new Connection("t5", "t7"),
      new Connection("t5", "t8"),
      new Connection("t5", "t9"),
      new Connection("a2", "t6"),
      new Connection("t6", "k5"),
      new Connection("t7", "k5"),
      new Connection("t8", "k6"),
      new Connection("t9", "k7")));
    CombinerDag combinerDag = new CombinerDag(dag, new HashSet<>(Arrays.asList("a1", "a2")), new GroupIdGenerator());
    Map<String, Set<String>> groups = combinerDag.groupNodes();
    Set<Set<String>> groupSet = new HashSet<>(groups.values());

    Set<Set<String>> expected = new HashSet<>();
    expected.add(new HashSet<>(Arrays.asList("t2", "k1", "k2", "k3")));
    expected.add(new HashSet<>(Arrays.asList("t8", "t9", "k6", "k7")));

    Assert.assertEquals(expected, groupSet);
  }

  /**
   * Deterministically generates group ids.
   */
  private static class GroupIdGenerator implements Supplier<String> {
    private int count = 0;

    @Override
    public String get() {
      return "group" + count++;
    }
  }
}
