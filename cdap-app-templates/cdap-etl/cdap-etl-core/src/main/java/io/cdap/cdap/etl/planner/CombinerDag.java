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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A special DAG that can combine nodes within the dag into groups.
 *
 * Certain nodes are marked as uncombinable nodes, meaning they cannot be grouped together with any other node.
 * Nodes are grouped together if they have a common input node, and if no uncombinable node is accessible from any
 * node in the group.
 *
 * In more intuitive language, branches with sinks attached to them are grouped together to try
 * and reduce the number of branches in the dag. This is used in Spark pipelines to consolidate multiple RDDs into
 * single RDDs in order to prevent reprocessing of data.
 */
public class CombinerDag extends Dag {
  private static final Logger LOG = LoggerFactory.getLogger(CombinerDag.class);
  private final Set<String> uncombinableNodes;
  private final Supplier<String> groupIdGenerator;
  private final Map<String, Set<String>> groups;

  public CombinerDag(Dag dag, Set<String> uncombinableNodes) {
    this(dag, uncombinableNodes, () -> UUID.randomUUID().toString());
  }

  @VisibleForTesting
  CombinerDag(Dag dag, Set<String> uncombinableNodes, Supplier<String> groupIdGenerator) {
    super(dag);
    this.uncombinableNodes = Collections.unmodifiableSet(new HashSet<>(uncombinableNodes));
    // this is primarily used to deterministically generate group ids for unit tests
    this.groupIdGenerator = groupIdGenerator;
    this.groups = new HashMap<>();
  }

  /**
   * Group nodes in the dag together. This method will mutate the dag, with new nodes inserted where the group
   * of nodes used to be. Groups have the following properties:
   *
   *   1. Contains only combinable nodes
   *   2. Contains at least two sinks
   *   3. Does not contain a source
   *   4. There are no outgoing connections from any node in the group to a node outside the group
   *   5. They do not overlap. Nodes in one group cannot belong to another group
   *   6. Nodes in the group must either only have inputs from outside the group, or must only have inputs from
   *      inside the group.
   *   7. Nodes in the group must all share a common ancestor without passing through an uncombinable node
   *
   * @return mapping of new node ids to the group of old node ids used to create the group.
   */
  public Map<String, Set<String>> groupNodes() {
    /*
        For example, suppose the input phase is:

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

        Where S is a source, stages that start with T are transforms, that start with A are aggregators,
        and that start with K are sinks.
        The goal is to consolidate stages to end up with:

             |--> T1 --> G1
             |
             |
         S --|--> T3 --|
             |         |--|
             |            |--> K4
             |         |--|
             |--> T4 --|                |--> A2 --> T6 --|
                       |                |                |--> K5
                       |                |--> T7 ---------|
                       |--> A1 --> T5 --|
                                        |--> G2

        G1 contains T2, K1, K2 and K3
        G2 contains T8, T9, K6, K7

        For each multi-output stage, check if any of the branches can be combined.

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
    for (String nodeId : getTopologicalOrder()) {
      if (getNodeOutputs(nodeId).size() < 2) {
        continue;
      }

      Set<Branch> candidateBranches = new HashSet<>();
      Set<Branch> uncombinableBranches = new HashSet<>();

      /*
         If the dag looks like:

                                  |--> A2 --> T6 --|
                                  |                |--> K5
                                  |--> T7 ---------|
                             T5 --|
                                  |--> T8 --> K6
                                  |
                                  |--> T9 --> K7

         candidateBranches = [T7, K5], [T8, K6], [T9, K7]
         uncombinableBranches = [A2, T6, K5]
       */
      for (String outputNode : getNodeOutputs(nodeId)) {
        Branch branch = new Branch(accessibleFrom(outputNode));
        if (branch.containsAny(uncombinableNodes)) {
          uncombinableBranches.add(branch);
        } else {
          candidateBranches.add(branch);
        }
      }

      /*
         At the start:

         candidateBranches = [T7, K5], [T8, K6], [T9, K7]
         uncombinableBranches = [A2, T6, K5]

         1st pass:
           [T7, K5] get marked as uncombinable because it intersects with [A2, T6, K5]

           candidateBranches = [T8, K6], [T9, K7]
           uncombinableBranches = [T7, K5], [A2, T6, K5]

         2nd pass:
           no change
       */
      while (true) {
        Set<Branch> newUncombinables = new HashSet<>();
        for (Branch candidateBranch : candidateBranches) {
          for (Branch uncombinableBranch : uncombinableBranches) {
            if (candidateBranch.containsAny(uncombinableBranch.nodes)) {
              newUncombinables.add(candidateBranch);
            }
          }
        }
        if (newUncombinables.isEmpty()) {
          break;
        }
        candidateBranches.removeAll(newUncombinables);
        uncombinableBranches.addAll(newUncombinables);
      }

      /*
         At this point we have:
           candidateBranches = [T8, K6], [T9, K7]

         so we want to add a new group for [T8, T9, K6, K7],
         and replace those nodes with a new generated node


                                  |--> A2 --> T6 --|
                                  |                |--> K5
                                  |--> T7 ---------|
                             T5 --|
                                  |--> T8 --> K6
                                  |
                                  |--> T9 --> K7

         transformed to:

                                  |--> A2 --> T6 --|
                                  |                |--> K5
                                  |--> T7 ---------|
                             T5 --|
                                  |--> G1

       */
      if (candidateBranches.size() < 2) {
        continue;
      }

      Set<String> newGroup = new HashSet<>();
      // add new group
      for (Branch candidateBranch : candidateBranches) {
        newGroup.addAll(candidateBranch.nodes);
      }

      mergeToExistingGroups(newGroup);
    }

    // add new nodes for each group, replacing the original nodes
    for (Map.Entry<String, Set<String>> group : groups.entrySet()) {
      replaceGroup(group.getKey(), group.getValue());
    }

    return Collections.unmodifiableMap(groups);
  }

  private void replaceGroup(String groupId, Set<String> group) {
    // get all incoming connections to the group
    Set<String> groupInputs = new HashSet<>();
    for (String node : group) {
      Set<String> nodeInputs = getNodeInputs(node);
      groupInputs.addAll(Sets.difference(nodeInputs, group));
    }

    nodes.add(groupId);
    sinks.add(groupId);
    for (String groupInput : groupInputs) {
      addConnection(groupInput, groupId);
    }

    for (String node : group) {
      removeNode(node);
    }
  }

  private void mergeToExistingGroups(Set<String> newGroup) {
    Set<String> intersectingGroups = new HashSet<>();
    Set<String> mergedGroup = new HashSet<>(newGroup);
    /*
        check if this new group intersects with any other group:

        for example, if the dag starts out as:

             |--> k1
        s1 --|
             |--> k2
                  ^
           s2 ----|
                  |--> k3

        When examining s1, k1 and k2 will be found to be a group.
        When examining s2, k2 and k3 will be found to be a group.
        Since these two groups overlap at k2, they should be combined into a larger group of k1, k2, and k3.
     */
    for (Map.Entry<String, Set<String>> existingGroupEntry : groups.entrySet()) {
      String groupId = existingGroupEntry.getKey();
      Set<String> existingGroup = existingGroupEntry.getValue();
      if (!Sets.intersection(newGroup, existingGroup).isEmpty()) {
        intersectingGroups.add(groupId);
        mergedGroup.addAll(existingGroup);
      }
    }

    /*
        validate that the group is valid. A valid group has the following properties:

         1. Contains only combinable nodes
         2. Contains at least two sinks
         3. Does not contain a source
         4. There are no outgoing connections from any node in the group to a node outside the group
         5. They do not overlap. Nodes in one group cannot belong to another group
         6. Nodes in the group must either only have inputs from outside the group, or must only have inputs from
            inside the group.
         7. Nodes in the group must all share a common ancestor without passing through an uncombinable node

        groups should always be valid unless there is a bug in algorithm
     */
    int numSinks = 0;
    for (String groupNode : mergedGroup) {
      if (uncombinableNodes.contains(groupNode)) {
        LOG.debug("Planner tried to create an invalid group containing uncombinable node {}. " +
                    "This group will be ignored.",
                  groupNode);
        return;
      }
      if (sinks.contains(groupNode)) {
        numSinks++;
      }

      Set<String> inputs = getNodeInputs(groupNode);
      Set<String> interGroupInputs = Sets.intersection(inputs, mergedGroup);
      Set<String> intraGroupInputs = Sets.difference(inputs, mergedGroup);
      if (!interGroupInputs.isEmpty() && !intraGroupInputs.isEmpty()) {
        LOG.debug("Planner tried to create an invalid group with nodes {}. " +
                    "Node {} has inputs from both within the group and from outside the group. " +
                    "This group will be ignored.", mergedGroup, groupNode);
        return;
      }

      Set<String> outputs = getNodeOutputs(groupNode);
      if (!Sets.difference(outputs, mergedGroup).isEmpty()) {
        LOG.debug("Planner tried to create an invalid group with nodes {}. " +
                    "Node {} is connected to a node outside the group. This group will be ignored.",
                  mergedGroup, groupNode);
        return;
      }
    }

    if (numSinks < 2) {
      LOG.debug("Planner tried to create a group using nodes {}, but it only contains {} sinks. " +
                  "This group will be ignored.", mergedGroup, numSinks);
      return;
    }

    groups.put(groupIdGenerator.get(), mergedGroup);

    for (String intersectingGroup : intersectingGroups) {
      groups.remove(intersectingGroup);
    }
  }

  /**
   * Primarily used for readability, to have sets of branches instead of sets of sets.
   */
  private static class Branch {
    private final Set<String> nodes;

    private Branch(Set<String> nodes) {
      this.nodes = nodes;
    }

    private boolean containsAny(Set<String> set) {
      return !Sets.intersection(nodes, set).isEmpty();
    }
  }
}
