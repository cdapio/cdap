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

import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.PluginSpec;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Takes a {@link PipelineSpec} and creates an execution plan from it.
 */
public class PipelinePlanner {
  private final Set<String> reduceTypes;
  private final Set<String> isolationTypes;
  private final Set<String> supportedPluginTypes;
  private final Set<String> actionTypes;

  public PipelinePlanner(Set<String> supportedPluginTypes, Set<String> reduceTypes, Set<String> isolationTypes,
                         Set<String> actionTypes) {
    this.reduceTypes = ImmutableSet.copyOf(reduceTypes);
    this.isolationTypes = ImmutableSet.copyOf(isolationTypes);
    this.supportedPluginTypes = ImmutableSet.copyOf(supportedPluginTypes);
    this.actionTypes = ImmutableSet.copyOf(actionTypes);
  }

  /**
   * Create an execution plan for the given logical pipeline. This is used for batch pipelines.
   * Though it may eventually be useful to mark windowing points for realtime pipelines.
   *
   * A plan consists of one or more phases, with connections between phases.
   * A connection between a phase indicates control flow, and not necessarily
   * data flow. This class assumes that it receives a valid pipeline spec.
   * That is, the pipeline has no cycles, all its nodes have unique names,
   * sources don't have any input, sinks don't have any output,
   * everything else has both an input and an output, etc.
   *
   * We start by inserting connector nodes into the logical dag,
   * which are used to mark boundaries between mapreduce jobs.
   * Each connector represents a node where we will need to write to a local dataset.
   *
   * Next, the logical pipeline is broken up into phases,
   * using the connectors as sinks in one phase, and a source in another.
   * After this point, connections between phases do not indicate data flow, but control flow.
   *
   * @param spec the pipeline spec, representing a logical pipeline
   * @return the execution plan
   */
  public PipelinePlan plan(PipelineSpec spec) {
    // go through the stages and examine their plugin type to determine which stages are reduce stages
    Set<String> reduceNodes = new HashSet<>();
    Set<String> isolationNodes = new HashSet<>();
    Set<String> actionNodes = new HashSet<>();

    Map<String, StageSpec> specs = new HashMap<>();

    // Map to hold the connection information from condition nodes to the first stage
    // they connect to. Condition information also includes whether the stage is connected
    // on the 'true' branch or the 'false' branch
    Map<String, ConditionBranches> conditionBranches = new HashMap<>();

    for (StageSpec stage : spec.getStages()) {
      String pluginType = stage.getPlugin().getType();
      if (reduceTypes.contains(pluginType)) {
        reduceNodes.add(stage.getName());
      }
      if (isolationTypes.contains(pluginType)) {
        isolationNodes.add(stage.getName());
      }
      if (actionTypes.contains(pluginType)) {
        // Collect all Action nodes from spec
        actionNodes.add(stage.getName());
      }
      if (Condition.PLUGIN_TYPE.equals(pluginType)) {
        conditionBranches.put(stage.getName(), new ConditionBranches(null, null));
      }
      specs.put(stage.getName(), stage);
    }

    // Map to hold set of stages to which there is a connection from a action stage.
    SetMultimap<String, String> outgoingActionConnections = HashMultimap.create();
    // Map to hold set of stages from which there is a connection to action stage.
    SetMultimap<String, String> incomingActionConnections = HashMultimap.create();

    Set<Connection> connectionsWithoutAction = new HashSet<>();

    Map<String, String> conditionChildToParent = new HashMap<>();


    // Remove the connections to and from Action nodes in the pipeline in order to build the
    // ConnectorDag. Since Actions can only occur before sources or after sink nodes, the creation
    // of the ConnectorDag should not be affected after removal of connections involving action nodes.
    for (Connection connection : spec.getConnections()) {

      if (conditionBranches.containsKey(connection.getFrom())) {
        if (conditionBranches.containsKey(connection.getTo())) {
          // conditions are chained
          conditionChildToParent.put(connection.getTo(), connection.getFrom());
        }

        // Outgoing connection from condition
        ConditionBranches branches = conditionBranches.get(connection.getFrom());
        String trueOutput;
        String falseOutput;
        if (connection.getCondition()) {
          trueOutput = connection.getTo();
          falseOutput = branches.getFalseOutput();
        } else {
          trueOutput = branches.getTrueOutput();
          falseOutput = connection.getTo();
        }

        conditionBranches.put(connection.getFrom(), new ConditionBranches(trueOutput, falseOutput));
      }

      if (actionNodes.contains(connection.getFrom()) || actionNodes.contains(connection.getTo())) {
        if (actionNodes.contains(connection.getFrom())) {
          // Source of the connection is Action node
          outgoingActionConnections.put(connection.getFrom(), connection.getTo());
        }

        if (actionNodes.contains(connection.getTo())) {
          // Destination of the connection is Action node
          incomingActionConnections.put(connection.getTo(), connection.getFrom());
        }

        // Skip connections to and from action nodes
        continue;
      }
      connectionsWithoutAction.add(connection);
    }


    if (connectionsWithoutAction.isEmpty()) {
      // Pipeline only contains Actions
      Set<Connection> phaseConnections = new HashSet<>();
      Map<String, PipelinePhase> phases = new HashMap<>();
      populateActionPhases(specs, actionNodes, phases, phaseConnections, outgoingActionConnections,
                           incomingActionConnections, new HashMap<String, Dag>());
      return new PipelinePlan(phases, phaseConnections);
    }

    Set<Dag> splittedDags;
    Map<String, String> connectorNodes = new HashMap<>();
    if (conditionBranches.keySet().isEmpty()) {
      // insert connector stages into the logical pipeline
      ConnectorDag cdag = ConnectorDag.builder()
        .addConnections(connectionsWithoutAction)
        .addReduceNodes(reduceNodes)
        .addIsolationNodes(isolationNodes)
        .build();
      cdag.insertConnectors();
      connectorNodes.putAll(cdag.getConnectors());
      splittedDags = new HashSet<>(cdag.split());
    } else {
      splittedDags = split(connectionsWithoutAction, conditionBranches.keySet(), reduceNodes, isolationNodes,
                           connectorNodes);
    }

    // convert to objects the programs expect.
    Map<String, PipelinePhase> phases = new HashMap<>();

    // now split the logical pipeline into pipeline phases, using the connectors as split points
    Map<String, Dag> subdags = new HashMap<>();
    // assign some name to each subdag
    for (Dag subdag : splittedDags) {
      String name = getPhaseName(subdag.getSources(), subdag.getSinks());
      subdags.put(name, subdag);
    }

    // build connections between phases
    Set<Connection> phaseConnections = new HashSet<>();
    for (Map.Entry<String, Dag> subdagEntry1 : subdags.entrySet()) {
      String dag1Name = subdagEntry1.getKey();
      Dag dag1 = subdagEntry1.getValue();

      for (Map.Entry<String, Dag> subdagEntry2: subdags.entrySet()) {
        String dag2Name = subdagEntry2.getKey();
        Dag dag2 = subdagEntry2.getValue();
        if (dag1Name.equals(dag2Name)) {
          continue;
        }

        // if dag1 has any sinks that are a source in dag2, add a connection between the dags
        if (Sets.intersection(dag1.getSinks(), dag2.getSources()).size() > 0) {
          Set<String> conditionAsSink = Sets.intersection(dag1.getSinks(), conditionBranches.keySet());
          if (!conditionAsSink.isEmpty()) {
            // dag1 ends with condition
            // here we need to add new phase corresponding to the destination condition
            // also we need to add phase connections with either true or false from dag1 to dag2
            if (conditionAsSink.size() != 1) {
              // sink should only have a single stage if its condition
              throw new IllegalStateException(String.format("Dag '%s' to '%s' has condition as well " +
                                                              "as non-condition stages in sink which is not " +
                                                              "allowed.", dag1.getSources(), dag1.getSinks()));
            }
            String conditionName = conditionAsSink.iterator().next();
            createConditionPhase(conditionName, conditionBranches, dag1, dag1Name, dag2, dag2Name, phases,
                                 phaseConnections);
          } else {
            phaseConnections.add(new Connection(dag1Name, dag2Name));
          }
        }
      }
    }

    Map<String, String> conditionConnectors = getConnectorsAssociatedWithConditions(conditionBranches.keySet(),
                                                                                    conditionChildToParent);
    for (Map.Entry<String, Dag> dagEntry : subdags.entrySet()) {
      // If dag only contains conditions then ignore it
      if (conditionBranches.keySet().containsAll(dagEntry.getValue().getNodes())) {
        continue;
      }
      Dag dag = getUpdatedDag(dagEntry.getValue(), conditionConnectors);
      phases.put(dagEntry.getKey(), dagToPipeline(dag, connectorNodes, specs, conditionConnectors));
    }

    populateActionPhases(specs, actionNodes, phases, phaseConnections, outgoingActionConnections,
                         incomingActionConnections, subdags);

    return new PipelinePlan(phases, phaseConnections);
  }

  /**
   * This method is responsible for creating phases corresponding to the condition nodes. It also puts the phase
   * connections with appropriate true or false tags.
   */
  private void createConditionPhase(String conditionName, Map<String, ConditionBranches> conditionBranches,
                                    Dag dag1, String dag1Name, Dag dag2, String dag2Name,
                                    Map<String, PipelinePhase> phases, Set<Connection> phaseConnections) {
    Set<String> conditionNodes = conditionBranches.keySet();
    PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);
    PluginSpec conditionSpec = new PluginSpec(Condition.PLUGIN_TYPE, conditionName,
                                              new HashMap<String, String>(), null);
    PipelinePhase conditionPhase = phaseBuilder
      .addStage(StageSpec.builder(conditionName, conditionSpec).build())
      .build();
    phases.put(conditionName, conditionPhase);

    if (!conditionNodes.containsAll(dag1.getNodes())) {
      // dag1 does not only contain condition stages in it
      phaseConnections.add(new Connection(dag1Name, conditionName));
    }

    ConditionBranches branches = conditionBranches.get(conditionName);
    for (Boolean condition : Arrays.asList(true, false)) {
      String stage = condition ? branches.getTrueOutput() : branches.getFalseOutput();
      if (stage == null) {
        continue;
      }

      if (!dag2.getNodes().contains(stage)) {
        continue;
      }

      if (conditionNodes.containsAll(dag2.getNodes())) {
        // dag 2 only contains condition stages. dag1 sink is same as dag 2 source condition node.
        // so we added condition phase corresponding to the dag1 sink. Now phase connection should be
        // added from newly created condition phase to the dag 2 sink condition, rather than using dag2name
        // here. The scenario here is n1-c1-c2-c3-n2. dag1 is <n1, c1> and dag 2 is <c1, c2>. We just created
        // c1 as new phase. since dag2 satisfies the above if condition (as it only contains conditions) we add
        // phase connection from c1->c2, rather than c1->c1.to.c2
        phaseConnections.add(new Connection(conditionName, dag2.getSinks().iterator().next(),
                                            condition));
      } else {
        // scenario here is n1-c1-c2-n2. dag 1 is <c1, c2> and dag 2 is <c2, n2>. We just created condition phase
        // for c2 and the phase connection should be c2->c2.to.n2
        phaseConnections.add(new Connection(conditionName, dag2Name, condition));
      }
      break;
    }
  }

  /**
   * This method is responsible for returning {@link Map} of condition and associated connector name.
   * By default each condition will have associated connector named as conditionname.connector. This connector
   * will hold the data from the previeous phase. However it is possible that multiple conditions are
   * chained together. In this case we do not need to create individual connector for each of the child condition
   * but they should have connector same as parent condition.
   * @param conditionNodes Set of condition nodes in the spec
   * @param childToParent Map contains only conditions as keys. The corresponding value represents its immediate parent
   * @return the resolved connectors for each condition
   */
  private Map<String, String> getConnectorsAssociatedWithConditions(Set<String> conditionNodes,
                                                                    Map<String, String> childToParent) {
    Map<String, String> conditionConnectors = new HashMap<>();
    for (String condition : conditionNodes) {
      // Put the default connector for the condition
      conditionConnectors.put(condition, condition + ".connector");
    }

    for (Map.Entry<String, String> entry : childToParent.entrySet()) {
      String parent = childToParent.get(entry.getValue());
      conditionConnectors.put(entry.getKey(), entry.getValue() + ".connector");
      while (parent != null) {
        conditionConnectors.put(entry.getKey(), parent + ".connector");
        parent = childToParent.get(parent);
      }
    }
    return conditionConnectors;
  }

  /**
   * This method is responsible for populating phases and phaseConnections with the Action phases.
   * Action phase is a single stage {@link PipelinePhase} which does not have any dag.
   *
   * @param specs the Map of stage specs
   * @param actionNodes the Set of action nodes in the pipeline
   * @param phases the Map of phases created so far
   * @param phaseConnections the Set of connections between phases added so far
   * @param outgoingActionConnections the Map that holds set of stages to which
   *                                  there is an outgoing connection from a Action stage
   * @param incomingActionConnections the Map that holds set of stages to which
   *                                  there is a incoming connection to an Action stage
   * @param subdags subdags created so far from the pipeline stages
   */
  private void populateActionPhases(Map<String, StageSpec> specs, Set<String> actionNodes,
                                    Map<String, PipelinePhase> phases, Set<Connection> phaseConnections,
                                    SetMultimap<String, String> outgoingActionConnections,
                                    SetMultimap<String, String> incomingActionConnections, Map<String, Dag> subdags) {

    // Create single stage phases for the Action nodes
    for (String node : actionNodes) {
      StageSpec actionStageSpec = specs.get(node);
      phases.put(node, PipelinePhase.builder(supportedPluginTypes).addStage(actionStageSpec).build());
    }

    // Build phaseConnections for the Action nodes
    for (String sourceAction : outgoingActionConnections.keySet()) {
      // Check if destination is one of the source stages in the pipeline
      for (Map.Entry<String, Dag> subdagEntry : subdags.entrySet()) {
        if (Sets.intersection(outgoingActionConnections.get(sourceAction),
                              subdagEntry.getValue().getSources()).size() > 0) {
          phaseConnections.add(new Connection(sourceAction, subdagEntry.getKey()));
        }
      }

      // Check if destination is other Action node
      for (String destination : outgoingActionConnections.get(sourceAction)) {
        if (actionNodes.contains(destination)) {
          phaseConnections.add(new Connection(sourceAction, destination));
        }
      }
    }

    // At this point we have build phaseConnections from Action node to another Action node or phaseConnections
    // from Action node to another subdags. However it is also possible that sudags connects to the action node.
    // Build those connections here.

    for (String destinationAction : incomingActionConnections.keySet()) {
      // Check if source is one of the sink stages in the pipeline
      for (Map.Entry<String, Dag> subdagEntry : subdags.entrySet()) {
        if (Sets.intersection(incomingActionConnections.get(destinationAction),
                              subdagEntry.getValue().getSinks()).size() > 0) {
          phaseConnections.add(new Connection(subdagEntry.getKey(), destinationAction));
        }
      }
    }
  }

  /**
   * Update the current dag by replacing conditions in the dag with the corresponding condition connectors
   */
  private Dag getUpdatedDag(Dag dag, Map<String, String> conditionConnectors) {
    Set<String> conditionsAsSources = Sets.intersection(conditionConnectors.keySet(), dag.getSources());
    Set<String> conditionsAsSinks =  Sets.intersection(conditionConnectors.keySet(), dag.getSinks());
    if (conditionsAsSources.isEmpty() && conditionsAsSinks.isEmpty()) {
      return dag;
    }

    Set<Connection> newConnections = new HashSet<>();

    for (String node : dag.getNodes()) {
      String newNode = conditionConnectors.get(node) == null ? node : conditionConnectors.get(node);
      for (String inputNode : dag.getNodeInputs(node)) {
        newConnections.add(new Connection(conditionConnectors.get(inputNode) == null ? inputNode
                                            : conditionConnectors.get(inputNode), newNode));
      }

      for (String outputNode : dag.getNodeOutputs(node)) {
        newConnections.add(new Connection(newNode, conditionConnectors.get(outputNode) == null ? outputNode
          : conditionConnectors.get(outputNode)));
      }
    }

    return new Dag(newConnections);
  }

  /**
   * Converts a Dag into a PipelinePhase, using what we know about the plugin type of each node in the dag.
   * The PipelinePhase is what programs will take as input, and keeps track of sources, transforms, sinks, etc.
   *
   * @param dag the dag to convert
   * @param connectors connector nodes across all dags
   * @param specs specifications for every stage
   * @return the converted dag
   */
  private PipelinePhase dagToPipeline(Dag dag, Map<String, String> connectors, Map<String, StageSpec> specs,
                                      Map<String, String> conditionConnectors) {
    PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);

    for (String stageName : dag.getTopologicalOrder()) {
      Set<String> outputs = dag.getNodeOutputs(stageName);
      if (!outputs.isEmpty()) {
        phaseBuilder.addConnections(stageName, outputs);
      }

      // add connectors
      String originalName = connectors.get(stageName);
      if (originalName != null || conditionConnectors.values().contains(stageName)) {
        PluginSpec connectorSpec =
          new PluginSpec(Constants.CONNECTOR_TYPE, "connector",
                         ImmutableMap.of(Constants.CONNECTOR_ORIGINAL_NAME, originalName != null
                           ? originalName : stageName), null);
        phaseBuilder.addStage(StageSpec.builder(stageName, connectorSpec).build());
        continue;
      }

      // add other plugin types
      StageSpec spec = specs.get(stageName);
      phaseBuilder.addStage(spec);
    }

    return phaseBuilder.build();
  }

  @VisibleForTesting
  static String getPhaseName(Set<String> sources, Set<String> sinks) {
    // using sorted sets to guarantee the name is deterministic
    return Joiner.on('.').join(new TreeSet<>(sources)) +
      ".to." +
      Joiner.on('.').join(new TreeSet<>(sinks));
  }

  @VisibleForTesting
  static Set<Dag> split(Set<Connection> connections, Set<String> conditions, Set<String> reduceNodes,
                        Set<String> isolationNodes, Map<String, String> connectorNodes) {
    Dag dag = new Dag(connections);
    Set<Dag> subdags = dag.splitByConditions(conditions);

    Set<Dag> result = new HashSet<>();
    for (Dag subdag : subdags) {
      Set<String> subdagReduceNodes = Sets.intersection(reduceNodes, subdag.getNodes());
      Set<String> subdagIsolationNodes = Sets.intersection(isolationNodes, subdag.getNodes());

      ConnectorDag cdag = ConnectorDag.builder()
        .addDag(subdag)
        .addReduceNodes(subdagReduceNodes)
        .addIsolationNodes(subdagIsolationNodes)
        .build();

      cdag.insertConnectors();
      connectorNodes.putAll(cdag.getConnectors());
      result.addAll(cdag.split());
    }
    return result;
  }
}
