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
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ControlDagTest {

  @Test
  public void testNoOpTrim() {
    /*
                           |--> n4
        n1 --> n2 --> n3 --|
                           |--> n5
     */
    ControlDag cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n3", "n4"),
        new Connection("n3", "n5")));
    Assert.assertEquals(0, cdag.trim());

    /*
             |--> n2 --|
        n1 --|         |--> n4
             |--> n3 --|
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n2", "n4"),
        new Connection("n3", "n4")));
    Assert.assertEquals(0, cdag.trim());

    /*
                |--> n2 --|
             |--|         |
             |  |--> n3 --|--> n5
        n1 --|            |
             |-----> n4---|
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n1", "n4"),
        new Connection("n2", "n5"),
        new Connection("n3", "n5"),
        new Connection("n4", "n5")));
    Assert.assertEquals(0, cdag.trim());

    /*
              |--> n2 --|
              |         |--> n5
         n1 --|--> n3 --|
              |
              |--> n4 --> n6
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n1", "n4"),
        new Connection("n2", "n5"),
        new Connection("n3", "n5"),
        new Connection("n4", "n6")));
    Assert.assertEquals(0, cdag.trim());
  }

  @Test
  public void testTrim() {
    /*
             |--> n2 --|
        n1 --|         v
             |-------> n3
     */
    ControlDag cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n2", "n3")));
    /*
        trims down to:
        n1 --> n2 --> n3
     */
    Assert.assertEquals(1, cdag.trim());
    ControlDag expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3")));
    Assert.assertEquals(expected, cdag);

    /*
                       |--> n3 --|
             |--> n2 --|         |--> n5
             |         |--> n4 --|    ^
        n1 --|              |         |
             |              v         |
             |--> n6 -----> n7 -------|
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n6"),
        new Connection("n2", "n3"),
        new Connection("n2", "n4"),
        new Connection("n3", "n5"),
        new Connection("n4", "n5"),
        new Connection("n4", "n7"),
        new Connection("n6", "n7"),
        new Connection("n7", "n5")));
    /*
       trims down to:
                       |--> n3 --> n5
             |--> n2 --|           ^
             |         |--> n4     |
        n1 --|              |      |
             |              v      |
             |--> n6 -----> n7 ----|
     */
    Assert.assertEquals(1, cdag.trim());
    expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n6"),
        new Connection("n2", "n3"),
        new Connection("n2", "n4"),
        new Connection("n3", "n5"),
        new Connection("n4", "n7"),
        new Connection("n6", "n7"),
        new Connection("n7", "n5")));
    Assert.assertEquals(expected, cdag);

    /*
             |--> n2 --> n3 ------------
        n1 --|           |      |      |
             |           v      v      |
             |---------> n4 --> n5 ----|
             |           |             v
             |-----------------------> n6
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n4"),
        new Connection("n1", "n6"),
        new Connection("n2", "n3"),
        new Connection("n3", "n4"),
        new Connection("n3", "n5"),
        new Connection("n3", "n6"),
        new Connection("n4", "n5"),
        new Connection("n4", "n6"),
        new Connection("n5", "n6")));
    /*
       trims down to:
        n1 --> n2 --> n3 --> n4 --> n5 --> n6
     */
    Assert.assertEquals(5, cdag.trim());
    expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3"),
        new Connection("n3", "n4"),
        new Connection("n4", "n5"),
        new Connection("n5", "n6")));
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testFlattenNoOp() {
    /*
        n1 --> n2 --> n3
     */
    ControlDag cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3")));
    cdag.flatten();
    ControlDag expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3")));
    Assert.assertEquals(expected, cdag);

    /*
             |--> n2 --|
             |         |         |--> n6 --|
        n1 --|--> n3 --|--> n5 --|         |--> n8
             |         |         |--> n7 --|
             |--> n4 --|
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n1", "n4"),
        new Connection("n2", "n5"),
        new Connection("n3", "n5"),
        new Connection("n4", "n5"),
        new Connection("n5", "n6"),
        new Connection("n5", "n7"),
        new Connection("n6", "n8"),
        new Connection("n7", "n8")));
    cdag.flatten();
    expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n1", "n4"),
        new Connection("n2", "n5"),
        new Connection("n3", "n5"),
        new Connection("n4", "n5"),
        new Connection("n5", "n6"),
        new Connection("n5", "n7"),
        new Connection("n6", "n8"),
        new Connection("n7", "n8")));
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testMultiSourceFlatten() {
    /*
        n1 --|
             |--> n3
        n2 --|
     */
    ControlDag cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n3"),
        new Connection("n2", "n3")));
    cdag.flatten();
    ControlDag expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1.n2", "n1"),
        new Connection("n1.n2", "n2"),
        new Connection("n1", "n3"),
        new Connection("n2", "n3")));
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testFlatten() {
    /*
                      |--> n3
            |--> n2 --|
            |         |--> n4
       n1 --|
            |
            |--> n5
     */
    ControlDag cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n5"),
        new Connection("n2", "n3"),
        new Connection("n2", "n4")));
    cdag.flatten();
    /*
            |--> n2 --|             |--> n3 --|
       n1 --|         |--> n2.n5 -->|         |--> n3.n4
            |--> n5 --|             |--> n4 --|
     */
    ControlDag expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n5"),
        new Connection("n2", "n2.n5"),
        new Connection("n5", "n2.n5"),
        new Connection("n2.n5", "n3"),
        new Connection("n2.n5", "n4"),
        new Connection("n3", "n3.n4"),
        new Connection("n4", "n3.n4")));
    Assert.assertEquals(expected, cdag);

    /*
                      |--> n3
            |--> n2 --|
            |         |--> n4
       n1 --|              |
            |              v
            |--> n5 -----> n6
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n5"),
        new Connection("n2", "n3"),
        new Connection("n2", "n4"),
        new Connection("n4", "n6"),
        new Connection("n5", "n6")));
    cdag.flatten();
    /*


            |--> n2 --|
            |         |            |--> n3 ---------|
       n1 --|         |--> n2.n5 --|                |--> n3.n6
            |         |            |--> n4 --> n6 --|
            |--> n5 --|
     */
    expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n5"),
        new Connection("n2", "n2.n5"),
        new Connection("n5", "n2.n5"),
        new Connection("n2.n5", "n3"),
        new Connection("n2.n5", "n4"),
        new Connection("n4", "n6"),
        new Connection("n3", "n3.n6"),
        new Connection("n6", "n3.n6")));
    Assert.assertEquals(expected, cdag);

    /*
              |--> n2 --|
              |         |--> n5
         n1 --|--> n3 --|
              |
              |--> n4 --> n6
     */
    cdag = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n1", "n4"),
        new Connection("n2", "n5"),
        new Connection("n3", "n5"),
        new Connection("n4", "n6")));
    cdag.flatten();

    /*
              |--> n2 ---------|
              |                |
         n1 --|--> n3 ---------|--> n2.n3.n6 --> n5
              |                |
              |--> n4 --> n6 --|
     */
    expected = new ControlDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n1", "n4"),
        new Connection("n4", "n6"),
        new Connection("n2", "n2.n3.n6"),
        new Connection("n3", "n2.n3.n6"),
        new Connection("n6", "n2.n3.n6"),
        new Connection("n2.n3.n6", "n5")));
    Assert.assertEquals(expected, cdag);
  }
}
