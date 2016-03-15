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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * A DAG (directed acyclic graph) where edges represent a happens-before relationship.
 * In these types of scenarios, certain edges may be redundant and can be removed.
 * This simplifies the dag into something that is much easier to work with if it needs
 * to be used as a fork-join type of dag for workflow execution.
 */
public class ControlDag extends Dag {
  private static final Set<String> EMPTY = ImmutableSet.of();
  private final Multiset<String> nodeVisits;

  public ControlDag(Collection<Connection> connections) {
    super(connections);
    this.nodeVisits = HashMultiset.create();
  }

  /**
   * Record that this node was visited.
   *
   * @param node node that was visited
   * @return the number of times this node was visited, including the visit from this call
   */
  public int visit(String node) {
    nodeVisits.add(node);
    return nodeVisits.count(node);
  }

  /**
   * Resets the number of times each node was visited back to 0.
   */
  public void resetVisitCounts() {
    nodeVisits.clear();
  }

  /**
   * Flattens the control dag to remove connections between branches of different forks, which would
   * make the dag unusable in pure fork-join workflows.
   *
   * For example the following dag is not a fork-join dag:
   *
   *      |--> n2 -------|
   *      |              |--> n5
   *      |--> n3 -------|
   *  n1--|           |
   *      |           v
   *      |--> n4 --> n6
   *
   *  There are many ways to turn this a fork-join while still respecting all happens-before relationships,
   *  but for simplicity we'll use an algorithm that doesn't have any nested forks and will turn the above into:
   *
   *       |--> n2 --|
   *       |         |               |--> n5 --|
   *  n1 --|--> n3 --|--> n2.n3.n4 --|         |--> n5.n6
   *       |         |               |--> n6 --|
   *       |--> n4 --|
   *
   *  The algorithm is to insert a join node whenever it sees a fork. Every time there is a fork, we will follow
   *  each branch to its endpoint (a node that forks, merges, or is a sink), then insert a join node that each
   *  branch endpoint connects to.
   */
  public void flatten() {
    // this should never be the case, as it should be checked when the dag is created.
    if (sources.isEmpty()) {
      throw new IllegalStateException("There are no sources in the graph, which means there is a cycle.");
    }

    trim();
    String source;
    // if we have multiple sources, insert a fork node as the new source
    if (sources.size() > 1) {
      // copy to avoid concurrent modification
      Set<String> sourcesCopy = new HashSet<>(sources);
      String newId = generateJoinNodeName(sourcesCopy);
      addNode(newId, EMPTY, sourcesCopy);
      source = newId;
    } else {
      source = sources.iterator().next();
    }

    flattenFrom(source);
  }

  private void flattenFrom(String node) {
    Set<String> outputs = outgoingConnections.get(node);
    if (outputs.isEmpty()) {
      return;
    }

    if (outputs.size() == 1) {
      flattenFrom(outputs.iterator().next());
      return;
    }

    Multimap<String, String> branchEndpointOutputs = HashMultimap.create();
    // can't just use branchEndpointOutputs.keySet(),
    // because that won't track branch endpoints that had no output (sinks)
    Set<String> branchEndpoints = new HashSet<>();
    for (String output : outputs) {
      String branchEndpoint = findBranchEnd(output);
      branchEndpoints.add(branchEndpoint);
      branchEndpointOutputs.putAll(branchEndpoint, outgoingConnections.get(branchEndpoint));
    }

    // if all the branch endpoints connect to a single node, there is no need to add a join node
    Set<String> endpointOutputs = new HashSet<>(branchEndpointOutputs.values());
    if (endpointOutputs.size() == 1) {
      flattenFrom(endpointOutputs.iterator().next());
      return;
    }

    // add a connection from each branch endpoint to a newly added join node
    // then move all outgoing connections from each branch endpoint so that they are coming out of the new join node
    String newJoinNode = generateJoinNodeName(branchEndpoints);
    addNode(newJoinNode, branchEndpoints, endpointOutputs);
    // remove the outgoing connections from endpoints that aren't going to our new join node
    for (Map.Entry<String, String> endpointEntry : branchEndpointOutputs.entries()) {
      removeConnection(endpointEntry.getKey(), endpointEntry.getValue());
    }
    /*
       have to trim again due to reshuffling of nodes. For example, if we have:

                      |--> n3
            |--> n2 --|
            |         |--> n4
       n1 --|              |
            |              v
            |--> n5 -----> n6

       after we insert the new join node we'll have:

            |--> n2 --|           |--> n3
            |         |           |
       n1 --|         |--> join --|--> n4
            |         |           |    |
            |--> n5 --|           |    v
                                  |--> n6

       and we need to remove the connection from join -> n6, otherwise the algorithm will get messed up
     */
    trim();

    // then keep flattening from the new join node
    flattenFrom(newJoinNode);
  }

  // go down a branch until we find a node with multiple outputs, a node with multiple inputs, or a sink
  private String findBranchEnd(String node) {
    Set<String> outputs = outgoingConnections.get(node);
    // if this is a sink, or if this is a fork on a branch
    if (outputs.isEmpty() || outputs.size() > 1) {
      return node;
    }
    // if the next node is a join node
    String output = outputs.iterator().next();
    if (incomingConnections.get(output).size() > 1) {
      return node;
    }
    // otherwise keep going down this branch
    return findBranchEnd(output);
  }

  /**
   * Returns the number of paths from the start node to the stop node.
   * The number of paths from a node to itself is 1.
   *
   * @param start the node to start from
   * @param stop the node to end at
   * @return the number of paths from the start node to the stop node
   */
  private int numPaths(String start, String stop) {
    if (start.equals(stop)) {
      return 1;
    }
    int count = 0;
    for (String output : getNodeOutputs(start)) {
      count += numPaths(output, stop);
    }
    return count;
  }

  /**
   * Trims any redundant control connections.
   *
   * For example:
   *   n1 ------> n2
   *       |      |
   *       |      v
   *       |----> n3
   * has a redundant edge n1 -> n3, because the edge from n2 -> n3 already enforces n1 -> n3.
   * The approach is look at each node (call it nodeB). For each input into nodeB (call it nodeA),
   * if there is another path from nodeA to nodeB besides the direct edge, we can remove the edge nodeA -> nodeB.
   *
   * @return number of connections removed.
   */
  public int trim() {
    int numRemoved = 0;
    for (String node : nodes) {
      Set<Connection> toRemove = new HashSet<>();
      for (String nodeInput : getNodeInputs(node)) {
        if (numPaths(nodeInput, node) > 1) {
          toRemove.add(new Connection(nodeInput, node));
        }
      }
      for (Connection conn : toRemove) {
        removeConnection(conn.getFrom(), conn.getTo());
      }
      numRemoved += toRemove.size();
    }
    return numRemoved;
  }

  /**
   * Add a node with the following outputs and inputs
   */
  private void addNode(String node, Collection<String> inputs, Collection<String> outputs) {
    nodes.add(node);
    for (String output : outputs) {
      outgoingConnections.put(node, output);
      incomingConnections.put(output, node);
      sources.remove(output);
    }
    for (String input : inputs) {
      incomingConnections.put(node, input);
      outgoingConnections.put(input, node);
      sinks.remove(input);
    }
    if (outputs.isEmpty()) {
      sinks.add(node);
    }
    if (inputs.isEmpty()) {
      sources.add(node);
    }
  }

  private String generateJoinNodeName(Set<String> inputs) {
    // using sorted sets to guarantee the name is deterministic
    String name = Joiner.on('.').join(new TreeSet<>(inputs));
    if (nodes.contains(name)) {
      name += UUID.randomUUID().toString();
    }
    return name;
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

    ControlDag that = (ControlDag) o;

    return Objects.equals(nodeVisits, that.nodeVisits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), nodeVisits);
  }

  @Override
  public String toString() {
    return "ControlDag{" +
      "nodeVisits=" + nodeVisits +
      "} " + super.toString();
  }
}
