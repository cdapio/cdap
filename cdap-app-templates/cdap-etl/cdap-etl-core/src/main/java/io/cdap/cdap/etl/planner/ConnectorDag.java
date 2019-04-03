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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * A special DAG that can insert connector nodes into a normal dag based on some rules.
 * A connector node is a boundary at which the dag can be split into smaller dags.
 * A connector basically translates to a local dataset in between mapreduce jobs in the final workflow.
 */
public class ConnectorDag extends Dag {
  private final Set<String> reduceNodes;
  private final Set<String> isolationNodes;
  private final Set<String> multiPortNodes;
  // node name -> original node it was placed in front of
  private final Map<String, String> connectors;

  private ConnectorDag(Collection<Connection> connections,
                       Set<String> reduceNodes,
                       Set<String> isolationNodes,
                       Set<String> multiPortNodes,
                       Map<String, String> connectors) {
    super(connections);
    this.reduceNodes = ImmutableSet.copyOf(reduceNodes);
    this.isolationNodes = ImmutableSet.copyOf(isolationNodes);
    this.multiPortNodes = ImmutableSet.copyOf(multiPortNodes);
    this.connectors = new HashMap<>(connectors);
  }

  public ConnectorDag(Dag dag, Set<String> reduceNodes, Set<String> isolationNodes, Set<String> multiPortNodes,
                      Map<String, String> connectors) {
    super(dag);
    this.reduceNodes = ImmutableSet.copyOf(reduceNodes);
    this.isolationNodes = ImmutableSet.copyOf(isolationNodes);
    this.multiPortNodes = ImmutableSet.copyOf(multiPortNodes);
    this.connectors = new HashMap<>(connectors);
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
        or to multiple reduce nodes. A connector counts as both a source and a sink.

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
      if (!sources.contains(node) && !connectors.containsKey(node)) {
        continue;
      }

      Set<String> accessibleByNode = accessibleFrom(node, Sets.union(connectors.keySet(), reduceNodes));
      Set<String> sinksAndReduceNodes = Sets.intersection(
        accessibleByNode, Sets.union(connectors.keySet(), Sets.union(sinks, reduceNodes)));
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
      Set<String> accessibleByNode = accessibleFrom(reduceNode, Sets.union(connectors.keySet(), reduceNodes));
      Set<String> accessibleReduceNodes = Sets.intersection(accessibleByNode, reduceNodes);

      // Sets.difference because we don't want to add ourselves
      accessibleReduceNodes = Sets.difference(accessibleReduceNodes, ImmutableSet.of(reduceNode));
      for (String accessibleReduceNode : accessibleReduceNodes) {
        addConnectorInFrontOf(accessibleReduceNode, addedAlready);
      }
    }

    /*
        As an optimization, check if any connectors can be merged together.
        It is generally more performant to merge connectors if possible, as it will result in less I/O.
        To do this, we get the parents for all connectors, stopping at any source. Other connectors count as sources.
        Connectors can be merged if their branch inputs are exactly the same. For example, consider the following dag:

          s1 -----|  |--> t1 --> r1.connector --> r1 --> sink1
                  |--|
               |--|  |--> t2 --> r2.connector --> r2 --> sink2
          s2 --|
               |--> t3

        The branch for r1.connector is:

          t1 --> r1.connector --> r1 --> sink1

        with branch inputs [source1, source2]. The branch for r2.connector is:

          t2 --> r2.connector --> r2 --> sink2

        with branch inputs [source1, source2]. Since the branches have the same inputs, they can be merged to:

          s1 -----|                         |--> t1 --> r1 --> sink1
                  |-- s1.s2.out.connector --|
               |--|                         |--> t2  --> r2 --> sink2
          s2 --|
               |--> t3

        which will eventually be split to:

          s1 -----|                         s1.s2.out.connector --> t1 --> r1 --> sink1
                  |-- s1.s2.out.connector
               |--|                         s1.s2.out.connector --> t2 --> r2 --> sink2
          s2 --|
               |--> t3
     */
    // map contains connector branch head inputs -> connector heads
    Map<Set<String>, Set<ConnectorHead>> connectorsToMerge = new HashMap<>();
    // stop at reduce and isolation nodes. This is so that each branch will not contain multiple connectors
    Set<String> stopNodes = Sets.union(connectors.keySet(), Sets.union(isolationNodes, reduceNodes));
    for (String connector : connectors.keySet()) {
      List<String> branch = getBranch(connector, stopNodes);
      String branchHead = branch.iterator().next();
      Set<String> branchInputs = new HashSet<>(getNodeInputs(branchHead));
      // We don't want to merge connectors if any of the branch inputs is a multiPort node.
      // With a multiPort node, a connection to each connector is a subset of the full output.
      // With a normal node, a connection to each connector is the full output.
      // So in the normal node, it is fine to merge connections because the single connector will contain the
      // same data that each individual connector would have contained.
      // But with a multiPort node, each individual connector will contain different data, so they can't be merged.
      if (branchInputs.isEmpty() || !Sets.intersection(multiPortNodes, branchInputs).isEmpty()) {
        continue;
      }
      Set<ConnectorHead> connectorsWithSameInput = connectorsToMerge.get(branchInputs);
      if (connectorsWithSameInput == null) {
        connectorsWithSameInput = new HashSet<>();
        connectorsToMerge.put(branchInputs, connectorsWithSameInput);
      }
      connectorsWithSameInput.add(new ConnectorHead(connector, branchHead));
    }

    for (Map.Entry<Set<String>, Set<ConnectorHead>> entry : connectorsToMerge.entrySet()) {
      Set<String> branchInputs = entry.getKey();
      Set<ConnectorHead> toMerge = entry.getValue();
      if (toMerge.size() < 2) {
        continue;
      }
      /*
         At this point, we have something like:

          s1 -----|  |--> t1 --> r1.connector --> r1 --> sink1
                  |--|
               |--|  |--> t2 --> r2.connector --> r2 --> sink2
          s2 --|
               |--> t3

         Where branchInputs = [s1, s2] and toMerge = [r1.connector, r2.connector]

         We need to move and merge the connectors to end up with:

          s1 -----|                         |--> t1 --> r1.connector --> r1 --> sink1
                  |-- s1.s2.out.connector --|
               |--|                         |--> t2 --> r2.connector --> r2 --> sink2
          s2 --|
               |--> t3
      */
      // first add a connection from each branch input to a new connector node
      // sort the inputs so the connector name is deterministic to make it easier to read and test
      List<String> binputs = new ArrayList<>(branchInputs);
      Collections.sort(binputs);
      String connectorName = getConnectorName(Joiner.on(".").join(binputs).concat(".out"));
      nodes.add(connectorName);
      for (String branchInput : branchInputs) {
        addConnection(branchInput, connectorName);
      }
      connectors.put(connectorName, connectorName);
      for (ConnectorHead connectorHead : toMerge) {
        // add connection from new connector to branch heads
        addConnection(connectorName, connectorHead.branchHead);
        // remove connection from branch inputs to branch heads
        for (String branchInput : branchInputs) {
          removeConnection(branchInput, connectorHead.branchHead);
        }

        // remove the original connectors
        for (String connectorInput : getNodeInputs(connectorHead.connector)) {
          for (String connectorOutput : getNodeOutputs(connectorHead.connector)) {
            addConnection(connectorInput, connectorOutput);
          }
        }
        removeNode(connectorHead.connector);
        connectors.remove(connectorHead.connector);
      }
    }

    /*
        Find sinks that have at multiple reduce nodes as parents, or at least one reduce node and one source
        as parents. Connectors count as sources. We need to insert a connector in front of those sinks
        to ensure that a sink is never placed in multiple subdags when split. For example:

                   |--> reduce1 --|
          source --|              |--> sink
                   |--> reduce2 --|

        Will currently be broken down to:

                   |--> reduce1.connector    reduce1.connector --> reduce1 --> sink
          source --|
                   |--> reduce2.connector    reduce2.connector --> reduce2 --> sink

        This would make the sink in two different subdags so we insert a connector in front of the sink to make it:

                   |--> reduce1.connector    reduce1.connector --> reduce1 --> sink.connector
          source --|                                                                             sink.connector --> sink
                   |--> reduce2.connector    reduce2.connector --> reduce2 --> sink.connector
     */
    for (String sink : sinks) {
      Set<String> sourcesAndReduceNodes = Sets.union(connectors.keySet(), Sets.union(sources, reduceNodes));
      Set<String> parents = parentsOf(sink, sourcesAndReduceNodes);

      Set<String> parentSources = Sets.intersection(sourcesAndReduceNodes, parents);
      Set<String> reduceParents = Sets.intersection(parentSources, reduceNodes);
      // at least one reduce parent and at least two sources
      if (reduceParents.size() > 0 && parentSources.size() > 1) {
        addConnectorInFrontOf(sink, addedAlready);
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
      if (connectors.containsKey(input)) {
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
      if (connectors.containsKey(output)) {
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

  /**
   * @return map of connector nodes to the nodes they were original placed in front of
   */
  public Map<String, String> getConnectors() {
    return connectors;
  }

  /**
   * Split this dag into multiple dags. Each subdag will contain at most a single reduce node.
   *
   * @return list of subdags.
   */
  public List<Dag> split() {
    List<Dag> dags = new ArrayList<>();

    Set<String> remainingNodes = new HashSet<>(nodes);
    Set<String> possibleNewSources = Sets.union(sources, connectors.keySet());
    Set<String> possibleNewSinks = Sets.union(sinks, connectors.keySet());
    for (String reduceNode : reduceNodes) {
      Dag subdag = subsetAround(reduceNode, possibleNewSources, possibleNewSinks);
      // remove all non-connector sinks from remaining nodes.
      // connectors will be a source in one subdag and a sink in another subdag,
      // so they will all eventually be removed as a source, or will end up in remainingSources down below
      Set<String> subdagConnectorSinks = Sets.intersection(subdag.getSinks(), connectors.keySet());
      remainingNodes.removeAll(Sets.difference(subdag.getNodes(), subdagConnectorSinks));
      dags.add(subdag);
    }

    Set<String> remainingSources = new TreeSet<>(Sets.intersection(remainingNodes, possibleNewSources));

    /* Since there can be remaining sources from subdags which don't overlap, they should be split as seperate subdags.
     For example:

             n1 --|
                  |--- n3(r) ---|
             n2 --|             |-- n5(c) --- n5(r) --|
                    n4 ---------|                     |
                                                      |--- n7(c) --- n7(r) --- n8
                                                      |
                                          n6 ---------|
                                                      |
                                          n9 ---------|

      Here, remainingSources are n4, n6 and n9.

      Now, n6, n9, n7.connector should be in the same subdag and n4, n5.connector should be another subdag. So, the
      algorithm will first create dag1(n4, n5.connector), dag2(n6, n7.connector) and dag3(n9, n7.connector) and
      then it will merge dag2 and dag3 into (n6, n9, n7.connector) because they have n7.connector in common, and will
      keep (n4, n5.connector) as a separate dag since it does not overlap with nodes from dag2 and dag3.
    */

    // For that, we first create all the subdags from remaining sources and keep track of accessible nodes for those
    // subdags. source -> [ nodes accessible by the source ]
    Map<String, Dag> remainingDags = new HashMap<>();
    for (String remainingSource : remainingSources) {
      remainingDags.put(remainingSource, subsetFrom(remainingSource, possibleNewSinks));
    }

    // Then we merge overlapping subdags.
    Set<String> processedSources = new HashSet<>();
    for (String remainingSource : remainingSources) {
      // Skip remainingSource if it has already been added to a dag.
      if (!processedSources.add(remainingSource)) {
        continue;
      }

      Dag subdag = remainingDags.get(remainingSource);
      // Don't count the sources when looking for subdag overlap.
      // This is to prevent a subdag with a connector as a sink
      // and another subdag with that same connector as a source from getting merged
      Set<String> subdagNodes = new HashSet<>(subdag.getNodes());
      Set<String> nonSourceNodes = Sets.difference(subdagNodes, subdag.getSources());
      // go through all the other remaining sources and see if there is a path from them to our current dag
      Set<String> otherSources = Sets.difference(remainingSources, processedSources);
      // keep looping until no new nodes were added to the subdag.
      boolean nodesAdded;
      do {
        nodesAdded = false;
        for (String otherSource : otherSources) {
          Dag otherSubdag = remainingDags.get(otherSource);
          // If there is a path from the other source to our current dag, add those nodes to our current dag
          Set<String> otherNonSourceNodes = Sets.difference(otherSubdag.getNodes(), otherSubdag.getSources());
          // Don't count the sources when looking for subdag overlap.
          // This is to prevent a subdag with a connector as a sink
          // and another subdag with that same connector as a source from getting merged
          if (!Sets.intersection(nonSourceNodes, otherNonSourceNodes).isEmpty()) {
            if (subdagNodes.addAll(otherSubdag.getNodes())) {
              nodesAdded = true;
            }
          }
        }
      } while (nodesAdded);
      Dag mergedSubdag = createSubDag(subdagNodes);
      dags.add(mergedSubdag);
      // keep track of processed nodes
      processedSources.addAll(mergedSubdag.getSources());
    }
    return dags;
  }

  // add a connector in front of the specified node if one doesn't already exist there
  // returns the node in front of the specified node.
  private String addConnectorInFrontOf(String inFrontOf, Set<String> addedAlready) {
    if (!addedAlready.add(inFrontOf)) {
      return getNodeInputs(inFrontOf).iterator().next();
    }

    String connectorName = getConnectorName(inFrontOf);
    insertInFront(connectorName, inFrontOf);
    // in case we ever have a connector placed in front of another connector
    String original = connectors.containsKey(inFrontOf) ? connectors.get(inFrontOf) : inFrontOf;
    connectors.put(connectorName, original);
    return connectorName;
  }

  private String getConnectorName(String base) {
    String name = base + ".connector";
    if (nodes.contains(name)) {
      name += UUID.randomUUID().toString();
    }
    return name;
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

  /**
   * Just a container to hold a connector and the head of the branch it is on
   */
  private static class ConnectorHead {
    private final String connector;
    private final String branchHead;

    private ConnectorHead(String connector, String branchHead) {
      this.connector = connector;
      this.branchHead = branchHead;
    }
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
    private final Set<String> multiPortNodes;
    private final Map<String, String> connectors;
    private Dag dag;

    private Builder() {
      this.connections = new HashSet<>();
      this.reduceNodes = new HashSet<>();
      this.isolationNodes = new HashSet<>();
      this.multiPortNodes = new HashSet<>();
      this.connectors = new HashMap<>();
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

    public Builder addMultiPortNodes(String... nodes) {
      Collections.addAll(multiPortNodes, nodes);
      return this;
    }

    public Builder addMultiPortNodes(Collection<String> nodes) {
      multiPortNodes.addAll(nodes);
      return this;
    }

    public Builder addConnectors(String... nodes) {
      if (nodes.length % 2 != 0) {
        throw new IllegalArgumentException("must specify an even number of nodes, alternating between the " +
                                             "connector name and the original node it was placed in front of.");
      }
      for (int i = 0; i < nodes.length; i += 2) {
        connectors.put(nodes[i], nodes[i + 1]);
      }
      return this;
    }

    public Builder addConnection(String from, String to) {
      connections.add(new Connection(from, to));
      return this;
    }

    public Builder addConnections(Collection<Connection> connections) {
      if (dag != null) {
        throw new IllegalArgumentException("Must specify either connections or dag but not both.");
      }
      this.connections.addAll(connections);
      return this;
    }

    public Builder addDag(Dag dag) {
      if (!connections.isEmpty()) {
        throw new IllegalArgumentException("Must specify either connections or dag but not both.");
      }
      this.dag = dag;
      return this;
    }

    public ConnectorDag build() {
      if (dag == null) {
        return new ConnectorDag(connections, reduceNodes, isolationNodes, multiPortNodes, connectors);
      }
      return new ConnectorDag(dag, reduceNodes, isolationNodes, multiPortNodes, connectors);
    }
  }
}
