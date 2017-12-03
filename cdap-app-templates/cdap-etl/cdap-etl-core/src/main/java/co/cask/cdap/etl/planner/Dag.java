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
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * A DAG (directed acyclic graph).
 */
public class Dag {
  protected final Set<String> nodes;
  protected final Set<String> sources;
  protected final Set<String> sinks;
  // stage -> outputs of that stage
  protected final SetMultimap<String, String> outgoingConnections;
  // stage -> inputs for that stage
  protected final SetMultimap<String, String> incomingConnections;

  public Dag(Collection<Connection> connections) {
    Preconditions.checkArgument(!connections.isEmpty(), "Cannot create a DAG without any connections");
    this.outgoingConnections = HashMultimap.create();
    this.incomingConnections = HashMultimap.create();
    for (Connection connection : connections) {
      outgoingConnections.put(connection.getFrom(), connection.getTo());
      incomingConnections.put(connection.getTo(), connection.getFrom());
    }
    this.sources = new HashSet<>();
    this.sinks = new HashSet<>();
    this.nodes = new HashSet<>();
    init();
    validate();
  }

  protected Dag(SetMultimap<String, String> outgoingConnections, SetMultimap<String, String> incomingConnections) {
    this.outgoingConnections = HashMultimap.create(outgoingConnections);
    this.incomingConnections = HashMultimap.create(incomingConnections);
    this.sources = new HashSet<>();
    this.sinks = new HashSet<>();
    this.nodes = new HashSet<>();
    init();
  }

  protected Dag(Dag other) {
    this(other.outgoingConnections, other.incomingConnections);
  }

  /**
   * Validate the DAG is a valid DAG without cycles, and no islands. This should only be called before any
   * mutating operations like {@link #removeSource()} are called.
   *
   * @throws IllegalStateException if there is a cycle in the graph, or an island in the graph
   */
  void validate() {
    // if there are no sources, we must have a cycle.
    if (sources.isEmpty()) {
      throw new IllegalStateException("DAG does not have any sources. Please remove cycles from the graph.");
    }
    // similarly, if there are no sinks, we must have a cycle
    if (sinks.isEmpty()) {
      throw new IllegalStateException("DAG does not have any sinks. Please remove cycles from the graph.");
    }

    // check for cycles
    getTopologicalOrder();

    // check for sections of the dag that are on an island by themselves

    // source -> [ nodes accessible by the source ]
    Map<String, Set<String>> nodesAccessibleBySources = new HashMap<>();
    for (String source : sources) {
      nodesAccessibleBySources.put(source, accessibleFrom(source));
    }

    // the algorithm is to keep an island and try to keep adding to it until we can't anymore.
    // the island starts off as the nodes accessible by the first source
    // we then loop through all other sources and add them to the island if they can access any node in the island.
    // we stop if we ever loop through all other sources and can't add them to the island,
    // or if the island has grown to include all sources.
    Set<String> islandNodes = new HashSet<>();
    // seed the island with the first source
    Set<String> potentialIslandSources = new HashSet<>(sources);
    String firstSource = potentialIslandSources.iterator().next();
    islandNodes.addAll(nodesAccessibleBySources.get(firstSource));
    potentialIslandSources.remove(firstSource);

    while (!potentialIslandSources.isEmpty()) {
      Set<String> sourcesAdded = new HashSet<>();
      // for each source not yet a part of the island
      for (String potentialIslandSource : potentialIslandSources) {
        Set<String> accessibleBySource = nodesAccessibleBySources.get(potentialIslandSource);
        // if that source can access the island in any way, add it to the island
        if (!Sets.intersection(islandNodes, accessibleBySource).isEmpty()) {
          islandNodes.addAll(accessibleBySource);
          sourcesAdded.add(potentialIslandSource);
        }
      }
      // if this is empty, no sources were added to the island. That means the island really is an island.
      if (sourcesAdded.isEmpty()) {
        throw new DisjointConnectionsException(
          String.format("Invalid DAG. There is an island made up of stages %s (no other stages connect to them).",
                        Joiner.on(',').join(islandNodes)));
      }
      potentialIslandSources.removeAll(sourcesAdded);
    }
  }

  /**
   * Split the dag based on the input control nodes.
   * @param controlNodes set of conditions and actions based on which to split the Dag
   * @return set of splitted dags
   */
  public Set<Dag> splitByControlNodes(Set<String> controlNodes) {

    Set<Dag> dags = new HashSet<>();

    Dag copy = new Dag(this);
    /*
        Remove control nodes from the front of the dag until there are no more
        For example suppose we have:

                   |-- n0 --|
        control0 --|        |-- control1 -- n2 -- n3 -- control2
                   |-- n1 --|

        after this loop, we will have added subdags:

        1. control0 -> n0
        2. control0 -> n1

        and the remaining dag will be:

          n0 --|
               |-- control1 -- n2 -- n3 -- control2
          n1 --|
     */
    Set<String> controlSources = new HashSet<>(Sets.intersection(copy.sources, controlNodes));
    while (!controlSources.isEmpty()) {
      for (String controlSource : controlSources) {
        for (String output : copy.getNodeOutputs(controlSource)) {
          dags.add(createSubDag(ImmutableSet.of(controlSource, output)));
        }
        copy.removeNode(controlSource);
      }
      controlSources.clear();
      controlSources.addAll(Sets.intersection(copy.sources, controlNodes));
    }
    /*
        Remove control nodes from the end of the dag until there are no more
        if the remaining dag is:

          n0 --|
               |-- control1 -- n2 -- n3 -- control2
          n1 --|

        after this loop we will have added subdag:

        3. n3 -> control2

        and the remaining dag will be:

          n0 --|
               |-- control1 -- n2 -- n3
          n1 --|
     */
    Set<String> controlSinks = new HashSet<>(Sets.intersection(copy.sinks, controlNodes));
    while (!controlSinks.isEmpty()) {
      for (String controlSink : controlSinks) {
        for (String input : copy.getNodeInputs(controlSink)) {
          dags.add(createSubDag(ImmutableSet.of(input, controlSink)));
        }
        copy.removeNode(controlSink);
      }
      controlSinks.clear();
      controlSinks.addAll(Sets.intersection(copy.sources, controlNodes));
    }

    /*
       If we started out with something like:

         control0 -- node -- control1

       At this point we would have subdags:

       1. control0 -- node
       2. node -- control1

       And the remaining 'dag' would be:

         node

       So if there is just one node left, we can stop here.
     */
    if (copy.getNodes().size() < 2) {
      return dags;
    }

    // now we handle any control nodes in the middle of the dag

    /*
        this will get the dag from all sources to all sinks or control nodes
        if we have dag:

          n0 --|
               |-- control1 -- n2 -- n3
          n1 --|

        we will add subdag:

        4. n0 -> control1, n1 -> control1

        and the remaining dag will be:

          control1 -- n2 -- n3

        we also need to be careful, as at this point, the copy dag might disjoint, like:

          n0 -- n1

          n2 -- n3
     */
    Set<String> remainingControlNodes = Sets.intersection(copy.nodes, controlNodes);
    List<Set<String>> sourceSubdags = new ArrayList<>(copy.sources.size());
    // add one subdag for each source
    for (String source : copy.sources) {
      sourceSubdags.add(copy.accessibleFrom(source, remainingControlNodes));
    }
    // merge subdags if they intersect at any point
    boolean done = sourceSubdags.size() <= 1;
    while (!done) {
      List<Set<String>> mergedSubdags = new ArrayList<>();
      Iterator<Set<String>> subdagIter = sourceSubdags.iterator();
      mergedSubdags.add(subdagIter.next());

      // check if this subdag intersects with any of the others
      done = true;
      while (subdagIter.hasNext()) {
        Set<String> subdag = subdagIter.next();
        boolean merged = false;
        for (Set<String> mergedSubdag : mergedSubdags) {
          if (!Sets.intersection(mergedSubdag, subdag).isEmpty()) {
            // it intersects, merge them
            mergedSubdag.addAll(subdag);
            merged = true;
            done = false;
            break;
          }
        }
        // it didn't intersect with anything, leave it on its own
        if (!merged) {
          mergedSubdags.add(subdag);
        }
      }

      sourceSubdags = mergedSubdags;
    }
    // add all merged subdags
    for (Set<String> sourceSubdag : sourceSubdags) {
      dags.add(createSubDag(sourceSubdag));
    }

    /*
        Now we need to go through all remaining control nodes (which are guaranteed to be conditions at this point)
        and add subdags for each remaining condition.
        If we have:

          control1 -- n2 -- n3

        we will add subdag:

        5. control1 -- n2 -- n3

        Consider a more complicated example, where c1, c2, and c3 are conditions:

                       c1 - t1---agg1--agg2---sink1
                        |
                        ----c2 - sink2
                             |
                              ------c3 - sink3

        This next part would add subdags:

        5. c1 - t1 - agg1 - agg2 - sink2
        6. c1 - c2
        7. c2 - sink2
        8. c2 - c3
        9. c3 - sink
     */
    for (String controlNode : remainingControlNodes) {
      // For child we need to add two sub-dags corresponding to the true and false branch
      // Condition node has at least one and at max two output nodes
      Set<String> outputs = copy.getNodeOutputs(controlNode);
      for (String output : outputs) {
        if (remainingControlNodes.contains(output)) {
          // condition is attached to the condition so just return that path
          dags.add(createSubDag(new HashSet<>(Arrays.asList(controlNode, output))));
          continue;
        }
        Set<String> childNodes = accessibleFrom(output, remainingControlNodes);
        childNodes.add(controlNode);
        dags.add(createSubDag(childNodes));
      }
    }
    return dags;
  }

  public Set<String> getNodes() {
    return nodes;
  }

  public Set<String> getSources() {
    return Collections.unmodifiableSet(new TreeSet<>(sources));
  }

  public Set<String> getSinks() {
    return Collections.unmodifiableSet(new TreeSet<>(sinks));
  }

  public Set<String> getNodeOutputs(String node) {
    return Collections.unmodifiableSet(outgoingConnections.get(node));
  }

  public Set<String> getNodeInputs(String node) {
    return Collections.unmodifiableSet(incomingConnections.get(node));
  }


  /**
   * Return all stages accessible from a starting stage.
   *
   * @param stage the stage to start at
   * @return all stages accessible from that stage
   */
  public Set<String> accessibleFrom(String stage) {
    return accessibleFrom(stage, ImmutableSet.<String>of());
  }

  /**
   * Return all stages accessible from a starting stage, without going past any node in stopNodes.
   * The starting stage is not treated as a stop node, even if it is in the stop nodes set.
   *
   * @param stage the stage to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return all stages accessible from that stage
   */
  public Set<String> accessibleFrom(String stage, Set<String> stopNodes) {
    return accessibleFrom(ImmutableSet.of(stage), stopNodes);
  }

  /**
   * Return all stages accessible from the starting stages, without going past any node in stopNodes.
   *
   * @param stages the stages to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return all stages accessible from that stage
   */
  public Set<String> accessibleFrom(Set<String> stages, Set<String> stopNodes) {
    Set<String> accessible = new HashSet<>();
    for (String stage : stages) {
      accessible.addAll(traverseForwards(stage, accessible, stopNodes));
    }
    return accessible;
  }

  /**
   * Return all stages that are parents of an ending stage. Stage X is a parent of stage Y if there is a path
   * from X to Y.
   *
   * @param stage the stage to start at
   * @return all parents of that stage
   */
  public Set<String> parentsOf(String stage) {
    return parentsOf(stage, ImmutableSet.<String>of());
  }

  /**
   * Return all stages that are parents of an ending stage, without going past any node in stopNodes.
   * A stage counts as a parent of itself.
   *
   * @param stage the stage to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return all parents of that stage
   */
  public Set<String> parentsOf(String stage, Set<String> stopNodes) {
    Set<String> accessible = new HashSet<>();
    accessible.addAll(traverseBackwards(stage, accessible, stopNodes));
    return accessible;
  }

  /**
   * Return a subset of this dag starting from the specified stage.
   * This is equivalent to calling {@link #subsetFrom(String, Set)} with an empty set for stop nodes
   *
   * @param stage the stage to start at
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(String stage) {
    return subsetFrom(stage, ImmutableSet.<String>of());
  }

  /**
   * Return a subset of this dag starting from the specified stage, without going past any node in stopNodes.
   * This is equivalent to taking the nodes from {@link #accessibleFrom(String, Set)} and building a dag from them.
   *
   * @param stage the stage to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(String stage, Set<String> stopNodes) {
    return subsetFrom(ImmutableSet.of(stage), stopNodes);
  }

  /**
   * Return a subset of this dag starting from the specified stages.
   * This is equivalent to calling {@link #subsetFrom(Set, Set)} with an empty set for stop nodes
   *
   * @param stages the stage to start at
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(Set<String> stages) {
    return subsetFrom(stages, ImmutableSet.<String>of());
  }

  /**
   * Return a subset of this dag starting from the specified stage, without going past any node in stopNodes.
   * This is equivalent to taking the nodes from {@link #accessibleFrom(Set, Set)} and building a dag from them.
   *
   * @param stages the stages to start at
   * @param stopNodes set of nodes to stop traversal on
   * @return a dag created from the nodes accessible from the specified stage
   */
  public Dag subsetFrom(Set<String> stages, Set<String> stopNodes) {
    Set<String> nodes = accessibleFrom(stages, stopNodes);
    Set<Connection> connections = new HashSet<>();
    for (String node : nodes) {
      for (String outputNode : outgoingConnections.get(node)) {
        if (nodes.contains(outputNode)) {
          connections.add(new Connection(node, outputNode));
        }
      }
    }
    return new Dag(connections);
  }

  /**
   * Return a subset of this dag starting from the specified stage, without going past any node in the
   * child stop nodes and parent stop nodes. If the parent or child stop nodes contain the starting stage, it
   * will be ignored.
   * This is equivalent to taking the nodes from {@link #accessibleFrom(Set, Set)}, {@link #parentsOf(String, Set)},
   * and building a dag from them.
   *
   * @param stage the stage to start at
   * @param childStopNodes set of nodes to stop traversing forwards on
   * @param parentStopNodes set of nodes to stop traversing backwards on
   * @return a dag created from the stages given and child nodes of those stages and parent nodes of those stages.
   */
  public Dag subsetAround(String stage, Set<String> childStopNodes, Set<String> parentStopNodes) {
    Set<String> nodes = Sets.union(accessibleFrom(stage, childStopNodes), parentsOf(stage, parentStopNodes));
    Set<Connection> connections = new HashSet<>();
    for (String node : nodes) {
      for (String outputNode : outgoingConnections.get(node)) {
        if (nodes.contains(outputNode)) {
          connections.add(new Connection(node, outputNode));
        }
      }
    }
    return new Dag(connections);
  }

  /**
   * Creates dag from provided list of nodes
   * @param nodes list of nodes to create subdag
   * @return Dag with connections among nodes
   */
  public Dag createSubDag(Set<String> nodes) {
    Set<Connection> connections = new HashSet<>();
    for (String node : nodes) {
      for (String outputNode : outgoingConnections.get(node)) {
        if (nodes.contains(outputNode)) {
          connections.add(new Connection(node, outputNode));
        }
      }
    }
    return new Dag(connections);
  }

  /**
   * Get the dag in topological order.
   * The returned list guarantees that for each item in the list, that item has no path to an
   * item that comes before it in the list. In the process, if a cycle is found, an exception will be thrown.
   * Topological sort means we pop off a source from the dag, re-calculate sources, and continue until there
   * are no more nodes left. Popping will be done on a copy of the dag so that this is not a destructive operation.
   *
   * @return the dag in topological order
   * @throws IllegalStateException if there is a cycle in the dag
   */
  public List<String> getTopologicalOrder() {
    List<String> linearized = new ArrayList<>();

    Dag copy = new Dag(outgoingConnections, incomingConnections);
    String removed;
    while ((removed = copy.removeSource()) != null) {
      linearized.add(removed);
    }
    if (copy.outgoingConnections.isEmpty()) {
      return linearized;
    }
    // if we've run out of sources to remove, but there are still connections, that means there is a cycle.
    // remove all sinks so we can print out where the cycle is.
    do {
      removed = copy.removeSink();
    } while (removed != null);
    Set<String> cycle = accessibleFrom(copy.outgoingConnections.keySet().iterator().next());
    throw new IllegalStateException(
      String.format("Invalid DAG. Stages %s form a cycle.", Joiner.on(',').join(cycle)));
  }

  /**
   * Remove a source from the dag. New sources will be re-calculated after the source is removed.
   *
   * @return the removed source, or null if there were no sources to remove.
   */
  protected String removeSource() {
    if (sources.isEmpty()) {
      return null;
    }
    String source = sources.iterator().next();
    removeNode(source);
    return source;
  }

  /**
   * Remove the specified connection. Does not check that the connection actually exists.
   * It is possible to break apart the dag with this call.
   */
  protected void removeConnection(String from, String to) {
    outgoingConnections.remove(from, to);
    incomingConnections.remove(to, from);
  }

  /**
   * Add the specified connection. It is possible to create a cycle with this call.
   */
  protected void addConnection(String from, String to) {
    outgoingConnections.put(from, to);
    incomingConnections.put(to, from);
  }

  /**
   * Remove a specific node from the dag. Removing a node will remove all connections into the node and all
   * connection coming out of the node. Removing a node will also re-compute the sources and sinks of the dag.
   *
   * @param node the node to remove
   */
  private void removeNode(String node) {
    // for each node this output to: node -> outputNode
    for (String outputNode : outgoingConnections.removeAll(node)) {
      // remove the connection from this node to its output
      incomingConnections.remove(outputNode, node);
      // if the removal of that connection caused the output to become a source, add it as a source
      if (incomingConnections.get(outputNode).isEmpty()) {
        sources.add(outputNode);
      }
    }
    // for each node that output to this node: inputNode -> node
    for (String inputNode : incomingConnections.removeAll(node)) {
      // remove the connection from the input node to this node
      outgoingConnections.remove(inputNode, node);
      // if the removal of that connection caused the input node to become a sink, add it as a sink
      if (outgoingConnections.get(inputNode).isEmpty()) {
        sinks.add(inputNode);
      }
    }
    // in case this node we removed a source or a sink (or both).
    sinks.remove(node);
    sources.remove(node);
    nodes.remove(node);
  }

  private Set<String> traverseForwards(String stage, Set<String> alreadySeen, Set<String> stopNodes) {
    return traverse(stage, alreadySeen, stopNodes, outgoingConnections);
  }

  private Set<String> traverseBackwards(String stage, Set<String> alreadySeen, Set<String> stopNodes) {
    return traverse(stage, alreadySeen, stopNodes, incomingConnections);
  }

  private Set<String> traverse(String stage, Set<String> alreadySeen, Set<String> stopNodes,
                               SetMultimap<String, String> connections) {
    if (!alreadySeen.add(stage)) {
      return alreadySeen;
    }
    Collection<String> outputs = connections.get(stage);
    if (outputs.isEmpty()) {
      return alreadySeen;
    }
    for (String output : outputs) {
      if (stopNodes.contains(output)) {
        alreadySeen.add(output);
        continue;
      }
      alreadySeen.addAll(traverse(output, alreadySeen, stopNodes, connections));
    }
    return alreadySeen;
  }

  private String removeSink() {
    if (sinks.isEmpty()) {
      return null;
    }
    String sink = sinks.iterator().next();
    removeNode(sink);
    return sink;
  }

  private void init() {
    nodes.clear();
    sources.clear();
    sinks.clear();
    nodes.addAll(outgoingConnections.keySet());
    nodes.addAll(outgoingConnections.values());
    for (String node : nodes) {
      if (outgoingConnections.get(node).isEmpty()) {
        sinks.add(node);
      }
      if (incomingConnections.get(node).isEmpty()) {
        sources.add(node);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Dag that = (Dag) o;

    return Objects.equals(nodes, that.nodes) &&
      Objects.equals(sources, that.sources) &&
      Objects.equals(sinks, that.sinks) &&
      Objects.equals(outgoingConnections, that.outgoingConnections) &&
      Objects.equals(incomingConnections, that.incomingConnections);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodes, sources, sinks, outgoingConnections, incomingConnections);
  }

  @Override
  public String toString() {
    return "Dag{" +
      "nodes=" + nodes +
      ", sources=" + sources +
      ", sinks=" + sinks +
      ", outgoingConnections=" + outgoingConnections +
      ", incomingConnections=" + incomingConnections +
      '}';
  }
}
