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
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
  private final Set<String> isolationNodes;
  private final Set<String> connectors;

  private ConnectorDag(Collection<Connection> connections,
                       Set<String> reduceNodes,
                       Set<String> isolationNodes,
                       Set<String> connectors) {
    super(connections);
    this.reduceNodes = ImmutableSet.copyOf(reduceNodes);
    this.isolationNodes = ImmutableSet.copyOf(isolationNodes);
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
   * @return the nodes that had connectors inserted in front of them
   */
  public Set<String> insertConnectors() {
    // none of this is particularly efficient, but this should never be a bottleneck
    // unless we're dealing with very very large dags

    Set<String> addedAlready = new HashSet<>();

    /*
        Isolate the specified node by inserting a connector in front of and behind the node.
        If all inputs into the the node are sources, a connector will not be inserted in front.
        If all outputs from the node are sinks, a connector will not be inserted after.
        Other connectors count as both a source and a sink.
     */
    for (String isolationNode : isolationNodes) {
      isolate(isolationNode, addedAlready);
    }

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
      Set<String> sinksAndReduceNodes = Sets.intersection(
        accessibleByNode, Sets.union(connectors, Sets.union(sinks, reduceNodes)));
      // don't count this node
      sinksAndReduceNodes = Sets.difference(sinksAndReduceNodes, ImmutableSet.of(node));

      if (sinksAndReduceNodes.size() > 1) {
        for (String reduceNodeConnector : Sets.intersection(sinksAndReduceNodes, reduceNodes)) {
          addConnectorInFrontOf(reduceNodeConnector, addedAlready);
        }
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

  /**
   * Isolate the specified node by inserting a connector in front of and behind the node.
   * If all inputs into the the node are sources, a connector will not be inserted in front.
   * If all outputs from the node are sinks, a connector will not be inserted after.
   * Other connectors count as both a source and a sink.
   */
  private void isolate(String node, Set<String> addedAlready) {
    if (!nodes.contains(node)) {
      throw new IllegalArgumentException(String.format("Cannot isolate node %s because it is not in the dag.", node));
    }

    /*
         If an input into this node is not a source and not a connector,
         or if the input has another output besides this node, insert a connector in front of this node.

         For example, if we're isolating n2, we need to insert a connector in front if we have a dag like:

              |--> n2
         n1 --|
              |--> n3

         but not if our dag looks like:

         n1 --> n2 --> n3
     */
    boolean shouldInsert = false;
    for (String input : incomingConnections.get(node)) {
      if (connectors.contains(input)) {
        continue;
      }
      if (outgoingConnections.get(input).size() > 1 || !sources.contains(input)) {
        shouldInsert = true;
        break;
      }
    }
    if (shouldInsert) {
      addConnectorInFrontOf(node, addedAlready);
    }

    /*
         If an output of this node is not a connector and not a sink,
         or if the output has another input besides this node, insert a connector in front of the output.

         For example, if we are isolating n2 in:

           n1 --|
                |--> n3
           n2 --|

         we need to insert a connector in front of n3.
         But if our dag looks like:

         n1 --> n2 --> n3

         there is no need to insert a connector in front of n3 anymore.
     */
    Set<String> insertNodes = new HashSet<>();
    for (String output : outgoingConnections.get(node)) {
      if (connectors.contains(output)) {
        continue;
      }
      if (incomingConnections.get(output).size() > 1 || !sinks.contains(output)) {
        insertNodes.add(output);
      }
    }
    for (String insertNode : insertNodes) {
      addConnectorInFrontOf(insertNode, addedAlready);
    }
  }

  public Set<String> getConnectors() {
    return connectors;
  }

  /**
   * Split this dag into multiple dags. Each subdag will contain at most a single reduce node.
   *
   * @return list of subdags.
   */
  public List<Dag> split() {
    List<Dag> dags = new ArrayList<>();

    Set<String> remainingNodes = new HashSet<>();
    remainingNodes.addAll(nodes);
    Set<String> possibleNewSources = Sets.union(sources, connectors);
    Set<String> possibleNewSinks = Sets.union(sinks, connectors);
    for (String reduceNode : reduceNodes) {
      Dag subdag = subsetAround(reduceNode, possibleNewSources, possibleNewSinks);
      remainingNodes.removeAll(subdag.getNodes());
      dags.add(subdag);
    }

    Set<String> remainingSources = Sets.intersection(remainingNodes, possibleNewSources);
    if (!remainingSources.isEmpty()) {
      dags.add(subsetFrom(remainingSources, possibleNewSinks));
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
    insertInFront(connectorName, inFrontOf);
    connectors.add(connectorName);
    return connectorName;
  }

  /**
   * Inserts a node in front of the specified node.
   *
   * @param name the name of the new node
   * @param inFrontOf the node to insert in front of
   */
  private void insertInFront(String name, String inFrontOf) {
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


  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for a connector dag
   */
  public static class Builder {
    private final Set<Connection> connections;
    private final Set<String> reduceNodes;
    private final Set<String> isolationNodes;
    private final Set<String> connectors;

    private Builder() {
      this.connections = new HashSet<>();
      this.reduceNodes = new HashSet<>();
      this.isolationNodes = new HashSet<>();
      this.connectors = new HashSet<>();
    }

    public Builder addReduceNodes(String... nodes) {
      Collections.addAll(reduceNodes, nodes);
      return this;
    }

    public Builder addReduceNodes(Collection<String> nodes) {
      reduceNodes.addAll(nodes);
      return this;
    }

    public Builder addIsolationNodes(String... nodes) {
      Collections.addAll(isolationNodes, nodes);
      return this;
    }

    public Builder addIsolationNodes(Collection<String> nodes) {
      isolationNodes.addAll(nodes);
      return this;
    }

    public Builder addConnectors(String... nodes) {
      Collections.addAll(connectors, nodes);
      return this;
    }

    public Builder addConnectors(Collection<String> nodes) {
      connectors.addAll(nodes);
      return this;
    }

    public Builder addConnection(String from, String to) {
      connections.add(new Connection(from, to));
      return this;
    }

    public Builder addConnections(Collection<Connection> connections) {
      this.connections.addAll(connections);
      return this;
    }

    public ConnectorDag build() {
      return new ConnectorDag(connections, reduceNodes, isolationNodes, connectors);
    }
  }
}
