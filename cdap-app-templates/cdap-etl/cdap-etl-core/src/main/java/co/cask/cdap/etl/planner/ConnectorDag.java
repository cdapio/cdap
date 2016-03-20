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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * A special DAG that can insert connector nodes into a normal dag based on some rules.
 * A connector node is a boundary at which the dag can be split into smaller dags.
 * A connector basically translates to a local dataset in between mapreduce jobs in the final workflow.
 */
public class ConnectorDag extends Dag {
  private final Set<String> reduceNodes;
  private final Set<String> connectors;

  public ConnectorDag(Collection<Connection> connections) {
    this(connections, ImmutableSet.<String>of());
  }

  public ConnectorDag(Collection<Connection> connections, Set<String> reduceNodes) {
    this(connections, reduceNodes, ImmutableSet.<String>of());
  }

  public ConnectorDag(Collection<Connection> connections, Set<String> reduceNodes, Set<String> connectors) {
    super(connections);
    this.reduceNodes = ImmutableSet.copyOf(reduceNodes);
    this.connectors = new HashSet<>(connectors);
  }

  /**
   * Insert connector nodes into the dag.
   *
   * A connector node is a boundary at which the pipeline can be split into sub dags.
   * It is treated as a sink within one subdag and as a source in another subdag.
   * A connector is inserted in front of a reduce node (aggregator plugin type, etc)
   * when there is a path from some source to one or more reduce nodes or sinks.
   * This is required because in a single mapper, we can't write to both a sink and do a reduce.
   * We also can't have 2 reducers in a single mapreduce job.
   * A connector is also inserted in front of any node if the inputs into the node come from multiple sources.
   * A connector is also inserted in front of a reduce node that has another reduce node as its input.
   *
   * After splitting, the result will be a collection of subdags, with each subdag representing a single
   * mapreduce job (or possibly map-only job). Or in spark, each subdag would be a series of operations from
   * one rdd to another rdd.
   *
   * @return the connectors added
   */
  public Set<String> insertConnectors() {
    // none of this is particularly efficient, but this should never be a bottleneck
    // unless we're dealing with very very large dags

    Set<String> addedAlready = new HashSet<>();

    /*
        Find sections of the dag where a source is writing to both a sink and a reduce node
        or to multiple reduce nodes. a connector counts as both a source and a sink.

        for example, if a source is writing to both a sink and a reduce:

                    |---> sink1
          source ---|
                    |---> reduce ---> sink2

        we need to split this up into:

                    |---> sink1
          source ---|                    =>     connector ---> reduce ---> sink2
                    |---> connector

        The same logic applies if a source is writing to multiple reduce nodes. So if we run into this scenario,
        we will add a connector in front of all reduce nodes accessible from the source.
        When trying to find a path from a source to multiple reduce nodes, we also need to stop searching
        once we see a reduce node or a connector. Otherwise, every single reduce node would end up
        with a connector in front of it.
     */
    for (String node : getTopologicalOrder()) {
      if (!sources.contains(node) && !connectors.contains(node)) {
        continue;
      }

      Set<String> accessibleByNode = accessibleFrom(node, Sets.union(connectors, reduceNodes));
      Set<String> sinksAndReduceNodes = Sets.intersection(accessibleByNode, Sets.union(sinks, reduceNodes));
      // don't count this node
      sinksAndReduceNodes = Sets.difference(sinksAndReduceNodes, ImmutableSet.of(node));

      if (sinksAndReduceNodes.size() > 1) {
        for (String reduceNodeConnector : Sets.intersection(sinksAndReduceNodes, reduceNodes)) {
          addConnectorInFrontOf(reduceNodeConnector, addedAlready);
        }
      }
    }

    /*
        Find nodes that have input from multiple sources and add them to the connectors set.
        We can probably remove this part once we support multiple sources. Even though we don't support
        multiple sources today, the fact that we support forks means we have to deal with the multi-input case
        and break it down into separate phases. For example:

                |---> reduce1 ---|
          n1 ---|                |---> n2
                |---> reduce2 ---|

        From the previous section, both reduces will get a connector inserted in front:

                |---> reduce1.connector               reduce1.connector ---> reduce1 ---|
          n1 ---|                              =>                                       |---> n2
                |---> reduce2.connector               reduce2.connector ---> reduce2 ---|

        Since we don't support multi-input yet, we need to convert that further into 3 phases:

          reduce1.connector ---> reduce1 ---> n2.connector
                                                                    =>       sink.connector ---> n2
          reduce2.connector ---> reduce2 ---> n2.connector

        To find these nodes, we traverse the graph in order and keep track of sources that have a path to each node
        with a map of node -> [ sources that have a path to the node ]
        if we find that a node is accessible by more than one source, we insert a connector in front of it and
        reset all sources for that node to its connector
     */
    SetMultimap<String, String> nodeSources = HashMultimap.create();
    for (String source : sources) {
      nodeSources.put(source, source);
    }
    for (String node : getTopologicalOrder()) {
      if (sinks.contains(node)) {
        continue;
      }

      Set<String> connectedSources = nodeSources.get(node);
      /*
          If this node is a connector, replace all sources for this node with itself, since a connector is a source
          Taking the example above, we end up with:

            reduce1.connector ---> reduce1 ---|
                                              |---> n2
            reduce2.connector ---> reduce2 ---|

          When we get to n2, we need it to see that it has 2 sources: reduce1.connector and reduce2.connector
          So when get to reduce1.connector, we need to replace its source (n1) with itself.
          Similarly, when we get to reduce2.connector, we need to replaces its source (n1) with itself.
          If we didn't, when we got to n2, it would think its only source is n1, and we would
          miss the connector that should be inserted in front of it.
       */
      if (connectors.contains(node)) {
        connectedSources = new HashSet<>();
        connectedSources.add(node);
        nodeSources.replaceValues(node, connectedSources);
      }
      // if more than one source is connected to this node, then we need to insert a connector in front of this node.
      // its source should then be changed to the connector that was inserted in front of it.
      if (connectedSources.size() > 1) {
        String connectorNode = addConnectorInFrontOf(node, addedAlready);
        connectedSources = new HashSet<>();
        connectedSources.add(connectorNode);
        nodeSources.replaceValues(node, connectedSources);
      }
      for (String nodeOutput : getNodeOutputs(node)) {
        // propagate the source connected to me to all my outputs
        nodeSources.putAll(nodeOutput, connectedSources);
      }
    }

    /*
        Find reduce nodes that are accessible from other reduce nodes. For example:

          source ---> reduce1 ---> reduce2 ---> sink

        Needs to be broken down into:

          source ---> reduce1 ---> reduce2.connector      =>     reduce2.connector ---> reduce2 ---> sink
     */
    for (String reduceNode : reduceNodes) {
      Set<String> accessibleByNode = accessibleFrom(reduceNode, Sets.union(connectors, reduceNodes));
      Set<String> accessibleReduceNodes = Sets.intersection(accessibleByNode, reduceNodes);

      // Sets.difference because we don't want to add ourselves
      accessibleReduceNodes = Sets.difference(accessibleReduceNodes, ImmutableSet.of(reduceNode));
      for (String accessibleReduceNode : accessibleReduceNodes) {
        addConnectorInFrontOf(accessibleReduceNode, addedAlready);
      }
    }

    return addedAlready;
  }

  public Set<String> getConnectors() {
    return connectors;
  }

  /**
   * Split this dag into multiple dags. Each connector is used as a split point where it is a sink of one dag and the
   * source of another dag.
   *
   * @return list of dags split on connectors.
   */
  public List<Dag> splitOnConnectors() {
    List<Dag> dags = new ArrayList<>();
    for (String source : sources) {
      dags.add(subsetFrom(source, connectors));
    }
    for (String connector : connectors) {
      dags.add(subsetFrom(connector, connectors));
    }

    return dags;
  }

  // add a connector in front of the specified node if one doesn't already exist there
  // returns the node in front of the specified node.
  private String addConnectorInFrontOf(String inFrontOf, Set<String> addedAlready) {
    if (!addedAlready.add(inFrontOf)) {
      return getNodeInputs(inFrontOf).iterator().next();
    }

    String connectorName = inFrontOf + ".connector";
    if (nodes.contains(connectorName)) {
      connectorName += UUID.randomUUID().toString();
    }
    insertNode(connectorName, inFrontOf);
    connectors.add(connectorName);
    return connectorName;
  }

  /**
   * Inserts a node in front of the specified node.
   *
   * @param name the name of the new node
   * @param inFrontOf the node to insert in front of
   */
  private void insertNode(String name, String inFrontOf) {
    if (!nodes.contains(inFrontOf)) {
      throw new IllegalArgumentException(
        String.format("Cannot insert in front of node %s because it does not exist.", inFrontOf));
    }
    if (!nodes.add(name)) {
      throw new IllegalArgumentException(
        String.format("Cannot insert node %s because it already exists.", name));
    }

    Set<String> inputs = incomingConnections.get(inFrontOf);
    incomingConnections.putAll(name, inputs);
    for (String input : inputs) {
      outgoingConnections.remove(input, inFrontOf);
      outgoingConnections.put(input, name);
    }
    outgoingConnections.put(name, inFrontOf);
    incomingConnections.replaceValues(inFrontOf, ImmutableSet.of(name));
  }

  @Override
  public String toString() {
    return "ConnectorDag{" +
      "reduceNodes=" + reduceNodes +
      ", connectors=" + connectors +
      "} " + super.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ConnectorDag that = (ConnectorDag) o;

    return Objects.equals(reduceNodes, that.reduceNodes) &&
      Objects.equals(connectors, that.connectors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), reduceNodes, connectors);
  }

}
