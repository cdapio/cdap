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

import java.util.HashSet;
import java.util.Set;

/**
 */
public class ConnectorDagTest {

  @Test
  public void testMergeConnectors() {
    /*
        n1 --|
             |--- n3
        n2 --|
     */
    ConnectorDag cdag = new ConnectorDag(
      ImmutableSet.of(new Connection("n1", "n3"), new Connection("n2", "n3")));
    cdag.insertConnectors();
    // n3 is not a connector because it is a sink
    Assert.assertTrue(cdag.getConnectors().isEmpty());

    /*
        n1 --|
             |--- n3 --- n4
        n2 --|
     */
    cdag = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n3"),
        new Connection("n2", "n3"),
        new Connection("n3", "n4")));
    cdag.insertConnectors();
    // n3 should have a connector inserted in front of it
    ConnectorDag expected = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n3.connector"),
        new Connection("n2", "n3.connector"),
        new Connection("n3", "n4"),
        new Connection("n3.connector", "n3")),
      ImmutableSet.<String>of(), ImmutableSet.of("n3.connector"));
    Assert.assertEquals(expected, cdag);


    /*
        n1 --|
             |--- n4 ---|
        n2 --|          |-- n6 --- n9
             |--- n5 ---|
        n3 --|     |
                   |------ n7 ---- n10
                   |        |
                   |------------ n8 -- n11
     */
    cdag = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n4"),
        new Connection("n1", "n5"),
        new Connection("n2", "n4"),
        new Connection("n2", "n5"),
        new Connection("n3", "n4"),
        new Connection("n3", "n5"),
        new Connection("n4", "n6"),
        new Connection("n5", "n6"),
        new Connection("n5", "n7"),
        new Connection("n5", "n8"),
        new Connection("n6", "n9"),
        new Connection("n7", "n8"),
        new Connection("n7", "n10"),
        new Connection("n8", "n11")));
    cdag.insertConnectors();
    // n4 and n5 should have connectors in front since they have multiple inputs
    // n6 should also since it has input from n4 and n5
    // n7 should not since its only input is n5
    // n8 also should not since its input is n5 and n7, but n7 comes from n5.
    expected = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n4.connector"),
        new Connection("n1", "n5.connector"),
        new Connection("n2", "n4.connector"),
        new Connection("n2", "n5.connector"),
        new Connection("n3", "n4.connector"),
        new Connection("n3", "n5.connector"),
        new Connection("n4", "n6.connector"),
        new Connection("n5", "n6.connector"),
        new Connection("n5", "n7"),
        new Connection("n5", "n8"),
        new Connection("n6", "n9"),
        new Connection("n7", "n8"),
        new Connection("n7", "n10"),
        new Connection("n8", "n11"),
        new Connection("n6.connector", "n6"),
        new Connection("n5.connector", "n5"),
        new Connection("n4.connector", "n4")),
      ImmutableSet.<String>of(),
      ImmutableSet.of("n4.connector", "n5.connector", "n6.connector"));
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testReduceNodeConnectors() {
    /*
             |--- n2
        n1 --|
             |--- n3(r) --- n4
     */
    // n3 is a reduce node, which means n1 is writing to a sink (n2) and a stop node (n3),
    // so n3 should have a connector inserted in front of it
    ConnectorDag cdag = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n3", "n4")),
      ImmutableSet.of("n3"));
    cdag.insertConnectors();
    ConnectorDag expected = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3.connector"),
        new Connection("n3", "n4"),
        new Connection("n3.connector", "n3")),
      ImmutableSet.of("n3"),
      ImmutableSet.of("n3.connector"));
    Assert.assertEquals(expected, cdag);

    /*
             |--- n2
        n1 --|
             |--- n3 --- n4
     */
    // the same graph as above, but n3 is not a reduce node. In this scenario, nothing is a connector
    cdag = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n3", "n4")));
    cdag.insertConnectors();
    Assert.assertTrue(cdag.getConnectors().isEmpty());

    /*
        n1 --- n2(r) --- n3(r) --- n4(r) --- n5

        in this example, n3 and n4 should have connectors
     */
    cdag = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3"),
        new Connection("n3", "n4"),
        new Connection("n4", "n5")),
      ImmutableSet.of("n2", "n3", "n4"));
    cdag.insertConnectors();
    expected = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3.connector"),
        new Connection("n3", "n4.connector"),
        new Connection("n4.connector", "n4"),
        new Connection("n4", "n5"),
        new Connection("n3.connector", "n3")),
      ImmutableSet.of("n2", "n3", "n4"),
      ImmutableSet.of("n3.connector", "n4.connector"));
    Assert.assertEquals(expected, cdag);

    /*
                       |-- n3(r) -- n4
        n1 --- n2(r) --|
                       |-- n5 -- n6(r) -- n7

        in this example, n3 and n6 should have connectors
     */
    cdag = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3"),
        new Connection("n2", "n5"),
        new Connection("n3", "n4"),
        new Connection("n5", "n6"),
        new Connection("n6", "n7")),
      ImmutableSet.of("n2", "n3", "n6"));
    cdag.insertConnectors();
    expected = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n2", "n3.connector"),
        new Connection("n2", "n5"),
        new Connection("n3", "n4"),
        new Connection("n5", "n6.connector"),
        new Connection("n6", "n7"),
        new Connection("n3.connector", "n3"),
        new Connection("n6.connector", "n6")),
      ImmutableSet.of("n2", "n3", "n6"),
      ImmutableSet.of("n3.connector", "n6.connector"));
    Assert.assertEquals(expected, cdag);

    /*
             |--- n2(r) ----------|
             |                    |                                    |-- n10
        n1 --|--- n3(r) --- n5 ---|--- n6 --- n7(r) --- n8 --- n9(r) --|
             |                    |                                    |-- n11
             |--- n4(r) ----------|

        in this example, n2, n3, n4, n6, and n9 should all have connectors
        n2, n3, n4 all have connectors because n1 is writing to multiple reduce nodes
        n6 has a connector since it has inputs from multiple sources
        n9 has a connector because it is connected to reduce node n7
     */
    cdag = new ConnectorDag(
      ImmutableSet.of(
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
        new Connection("n9", "n11")),
      ImmutableSet.of("n2", "n3", "n4", "n7", "n9"));
    cdag.insertConnectors();
    expected = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2.connector"),
        new Connection("n1", "n3.connector"),
        new Connection("n1", "n4.connector"),
        new Connection("n2", "n6.connector"),
        new Connection("n3", "n5"),
        new Connection("n4", "n6.connector"),
        new Connection("n5", "n6.connector"),
        new Connection("n6", "n7"),
        new Connection("n7", "n8"),
        new Connection("n8", "n9.connector"),
        new Connection("n9", "n10"),
        new Connection("n9", "n11"),
        new Connection("n2.connector", "n2"),
        new Connection("n3.connector", "n3"),
        new Connection("n4.connector", "n4"),
        new Connection("n6.connector", "n6"),
        new Connection("n9.connector", "n9")),
      ImmutableSet.of("n2", "n3", "n4", "n7", "n9"),
      ImmutableSet.of("n2.connector", "n3.connector", "n4.connector", "n6.connector", "n9.connector"));
    Assert.assertEquals(expected, cdag);
  }

  @Test
  public void testSplitDag() {
    /*
             |--- n2(r) ----------|
             |                    |                                    |-- n10
        n1 --|--- n3(r) --- n5 ---|--- n6 --- n7(r) --- n8 --- n9(r) --|
             |                    |                                    |-- n11
             |--- n4(r) ----------|

        in this example, n2, n3, n4, n6, and n9 should all have connectors
        n2, n3, n4 all have connectors because n1 is writing to multiple reduce nodes
        n6 has a connector since it has inputs from multiple sources
        n9 has a connector because it is connected to reduce node n7
     */
    ConnectorDag cdag = new ConnectorDag(
      ImmutableSet.of(
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
        new Connection("n9", "n11")),
      ImmutableSet.of("n2", "n3", "n4", "n7", "n9"));
    cdag.insertConnectors();
    Set<Dag> actual = new HashSet<>(cdag.splitOnConnectors());
    // dag_source_sink(s)
    Dag dag1 = new Dag(
      ImmutableSet.of(
        new Connection("n1", "n2.connector"),
        new Connection("n1", "n3.connector"),
        new Connection("n1", "n4.connector")));
    Dag dag2 = new Dag(
      ImmutableSet.of(
        new Connection("n2.connector", "n2"),
        new Connection("n2", "n6.connector")));
    Dag dag3 = new Dag(
      ImmutableSet.of(
        new Connection("n3.connector", "n3"),
        new Connection("n3", "n5"),
        new Connection("n5", "n6.connector")));
    Dag dag4 = new Dag(
      ImmutableSet.of(
        new Connection("n4.connector", "n4"),
        new Connection("n4", "n6.connector")));
    Dag dag5 = new Dag(
      ImmutableSet.of(
        new Connection("n6.connector", "n6"),
        new Connection("n6", "n7"),
        new Connection("n7", "n8"),
        new Connection("n8", "n9.connector")));
    Dag dag6 = new Dag(
      ImmutableSet.of(
        new Connection("n9.connector", "n9"),
        new Connection("n9", "n10"),
        new Connection("n9", "n11")));
    Set<Dag> expected = ImmutableSet.of(dag1, dag2, dag3, dag4, dag5, dag6);
    Assert.assertEquals(expected, actual);


    /*
             |---> n2(r)
             |      |
        n1 --|      |
             |      v
             |---> n3(r) ---> n4

        n2 and n3 should have connectors inserted in front of them to become:

             |---> n2.connector ---> n2(r)
             |                        |
        n1 --|                        |
             |                        v
             |-------------------> n3.connector ---> n3(r) ---> n4
     */
    cdag = new ConnectorDag(
      ImmutableSet.of(
        new Connection("n1", "n2"),
        new Connection("n1", "n3"),
        new Connection("n2", "n3"),
        new Connection("n3", "n4")),
      ImmutableSet.of("n2", "n3"));
    cdag.insertConnectors();
    actual = new HashSet<>(cdag.splitOnConnectors());

    /*
             |--> n2.connector
        n1 --|
             |--> n3.connector
     */
    dag1 = new Dag(ImmutableSet.of(
      new Connection("n1", "n2.connector"),
      new Connection("n1", "n3.connector")));
    /*
        n2.connector --> n2 --> n3.connector
     */
    dag2 = new Dag(ImmutableSet.of(
      new Connection("n2.connector", "n2"),
      new Connection("n2", "n3.connector")));
    /*
        n3.connector --> n3 --> n4
     */
    dag3 = new Dag(ImmutableSet.of(
      new Connection("n3.connector", "n3"),
      new Connection("n3", "n4")));
    expected = ImmutableSet.of(dag1, dag2, dag3);
    Assert.assertEquals(expected, actual);
  }

}
