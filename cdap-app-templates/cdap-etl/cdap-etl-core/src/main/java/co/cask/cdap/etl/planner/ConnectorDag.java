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
   * We also can't reduce on multiple keys in the same mapper.
   * A connector is also inserted in front of any node if the inputs into the node come from multiple sources.
   * A connector is also inserted in front of a reduce node that has another reduce node as its input.
   *
   * @return the connectors added
   */
  public Set<String> insertConnectors() {
    // none of this is particularly efficient, but this should never be a bottleneck
    // unless we're dealing with very very large dags

    // node to insert in front of -> new connector node
    Set<String> addedAlready = new HashSet<>();

    // first pass, find sections of the dag where a source is writing to both a sink and a reduce node
    // or to multiple reduce nodes. a connector counts as both a source and a sink.
    for (String node : linearize()) {
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

    // next pass, find nodes that have input from multiple sources and add them to the connectors set.
    // a connector counts as a source

    // traverse the graph and keep track of sources that have a path to each node
    // mapping of node -> sources that have a path to the node.
    // a connector node is considered a source
    SetMultimap<String, String> nodeSources = HashMultimap.create();
    for (String source : sources) {
      nodeSources.put(source, source);
    }
    for (String connector : connectors) {
      nodeSources.put(connector, connector);
    }
    for (String node : linearize()) {
      if (sinks.contains(node)) {
        continue;
      }
      Set<String> connectedSources = nodeSources.get(node);
      // if more than one source is connected to this node, then this node is a connector.
      // keep track of it, and wipe its node sources to just contain itself
      if (connectedSources.size() > 1) {
        addConnectorInFrontOf(node, addedAlready);
        connectedSources = new HashSet<>();
        connectedSources.add(node);
        nodeSources.replaceValues(node, connectedSources);
      }
      for (String nodeOutput : getNodeOutputs(node)) {
        // don't propagate to connectors, since they are counted as source and replace any inputs they may have
        if (connectors.contains(nodeOutput)) {
          continue;
        }
        // propagate sources connected to me to all my outputs
        nodeSources.putAll(nodeOutput, connectedSources);
      }
    }

    // next pass, find reduce nodes that are connected to other reduce nodes
    for (String reduceNode : reduceNodes) {
      Set<String> accessibleByNode = accessibleFrom(reduceNode, Sets.union(connectors, reduceNodes));
      Set<String> accessibleReduceNodes = Sets.intersection(accessibleByNode, reduceNodes);

      // Sets.difference because we don't want to add ourselves
      accessibleReduceNodes = Sets.difference(accessibleReduceNodes, ImmutableSet.of(reduceNode));
      for (String accessibleReduceNode : Sets.difference(accessibleReduceNodes, ImmutableSet.of(reduceNode))) {
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

  private void addConnectorInFrontOf(String inFrontOf, Set<String> addedAlready) {
    if (!addedAlready.add(inFrontOf)) {
      return;
    }

    String connectorName = inFrontOf + ".connector";
    if (nodes.contains(connectorName)) {
      connectorName += UUID.randomUUID().toString();
    }
    insertNode(connectorName, inFrontOf);
    connectors.add(connectorName);
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
        String.format("Cannot insert in front node %s because it does not exist.", inFrontOf));
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
