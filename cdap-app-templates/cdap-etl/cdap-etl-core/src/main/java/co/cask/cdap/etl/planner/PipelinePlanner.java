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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collections;
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
    Set<String> allNodes = new HashSet<>();

    // Map to hold the connection information from condition nodes to the first stage
    // they connect to. Condition information also includes whether the stage is connected
    // on the 'true' branch or the 'false' branch
    Map<String, ConditionBranches> conditionBranches = new HashMap<>();
    Map<String, Set<String>> conditionOutputs = new HashMap<>();
    Map<String, Set<String>> conditionInputs = new HashMap<>();

    Map<String, StageSpec> specs = new HashMap<>();

    for (StageSpec stage : spec.getStages()) {
      String pluginType = stage.getPlugin().getType();
      allNodes.add(stage.getName());
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
        conditionOutputs.put(stage.getName(), new HashSet<String>());
        conditionInputs.put(stage.getName(), new HashSet<String>());
      }
      specs.put(stage.getName(), stage);
    }

    // Special case for action nodes when there is no connection between them
    if (spec.getConnections().isEmpty()) {
      // All nodes should be actions
      if (!actionNodes.containsAll(allNodes)) {
        throw new IllegalStateException("No connections are specified.");
      }

      Map<String, PipelinePhase> phases = new HashMap<>();
      for (String actionNode : actionNodes) {
        PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);
        PipelinePhase actionPhase = phaseBuilder
          .addStage(specs.get(actionNode))
          .build();
        phases.put(actionNode, actionPhase);
      }
      return new PipelinePlan(phases, new HashSet<Connection>());
    }

    // Set representing control nodes (Conditions and Actions)
    Set<String> controlNodes = Sets.union(actionNodes, conditionBranches.keySet());

    Map<String, String> conditionChildToParent = new HashMap<>();

    for (Connection connection : spec.getConnections()) {
      if (conditionBranches.containsKey(connection.getFrom())) {
        conditionOutputs.get(connection.getFrom()).add(connection.getTo());
      }

      if (conditionBranches.containsKey(connection.getTo())) {
        conditionInputs.get(connection.getTo()).add(connection.getFrom());
      }

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
    }

    Map<String, String> connectorNodes = new HashMap<>();
    // now split the logical pipeline into pipeline phases, using the connectors as split points
    Set<Dag> splittedDag = split(spec.getConnections(), conditionBranches.keySet(), reduceNodes, isolationNodes,
                                 actionNodes, connectorNodes);
    Map<String, String> controlConnectors = getConnectorsAssociatedWithConditions(conditionBranches.keySet(),
                                                                                  conditionChildToParent,
                                                                                  conditionInputs, conditionOutputs,
                                                                                  actionNodes);

    Map<String, Dag> subdags = new HashMap<>();
    for (Dag subdag : splittedDag) {
      String name = getPhaseName(subdag.getSources(), subdag.getSinks());
      subdags.put(name, subdag);
    }

    // build connections between phases and convert dags to PipelinePhase.
    Set<Connection> phaseConnections = new HashSet<>();
    Map<String, PipelinePhase> phases = new HashMap<>();
    for (Map.Entry<String, Dag> dagEntry1 : subdags.entrySet()) {
      String dag1Name = dagEntry1.getKey();
      Dag dag1 = dagEntry1.getValue();

      // convert the dag to a PipelinePhase
      // add a separate pipeline phase for each control node in the subdag
      Set<String> dag1ControlNodes = Sets.intersection(controlNodes, dag1.getNodes());
      for (String dag1ControlNode : dag1ControlNodes) {
        if (!phases.containsKey(dag1ControlNode)) {
          phases.put(dag1ControlNode,
                     PipelinePhase.builder(supportedPluginTypes).addStage(specs.get(dag1ControlNode)).build());
        }
      }
      // if there are non-control nodes in the subdag, add a pipeline phase for it
      if (!controlNodes.containsAll(dag1.getNodes())) {
        // the updated dag replaces conditions with the corresponding connector if applicable.
        Dag updatedDag = getUpdatedDag(dag1, controlConnectors);
        // Remove any control nodes from this dag
        if (!Sets.intersection(updatedDag.getNodes(), controlNodes).isEmpty()) {
          Set<String> nodes = Sets.difference(updatedDag.getNodes(), controlNodes);
          updatedDag = updatedDag.createSubDag(nodes);
        }
        phases.put(dag1Name, dagToPipeline(updatedDag, connectorNodes, specs, controlConnectors));
      }

      for (String controlSource : Sets.intersection(controlNodes, dag1.getSources())) {
        ConditionBranches branches = conditionBranches.get(controlSource);
        Boolean condition = branches == null ?  null : dag1.getNodes().contains(branches.getTrueOutput());
        for (String output : dag1.getNodeOutputs(controlSource)) {
          if (controlNodes.contains(output)) {
            // control source -> control node, add a phase connection between the control phases
            phaseConnections.add(new Connection(controlSource, output, condition));
          } else {
            // control source -> non-control nodes, add a phase connection from the control phase to this dag
            phaseConnections.add(new Connection(controlSource, dag1Name, condition));
          }
        }
      }

      // If we have a non-control node -> control sink, we need to add a phase connection
      // from this dag to the control phase
      for (String controlSink : Sets.intersection(controlNodes, dag1.getSinks())) {
        for (String input : dag1.getNodeInputs(controlSink)) {
          if (controlNodes.contains(input)) {
            // control node -> control-sink, add a phase connection between the control phases
            ConditionBranches branches = conditionBranches.get(input);
            Boolean condition = branches == null ?  null : dag1.getNodes().contains(branches.getTrueOutput());
            phaseConnections.add(new Connection(input, controlSink, condition));
          } else {
            // non-control node -> control-sink, add a phase connection from this dag to the control phase
            phaseConnections.add(new Connection(dag1Name, controlSink));
          }
        }
      }

      // find connected subdags (they have a source that is a sink in dag1)
      Set<String> nonControlSinks = Sets.difference(dag1.getSinks(), controlNodes);
      for (Map.Entry<String, Dag> dagEntry2 : subdags.entrySet()) {
        String dag2Name = dagEntry2.getKey();
        Dag dag2 = dagEntry2.getValue();
        if (dag1Name.equals(dag2Name)) {
          continue;
        }

        if (!Sets.intersection(nonControlSinks, dag2.getSources()).isEmpty()) {
          phaseConnections.add(new Connection(dag1Name, dag2Name));
        }
      }
    }

    return new PipelinePlan(phases, phaseConnections);
  }

  /**
   * This method is responsible for returning {@link Map} of condition and associated connector name.
   * By default each condition will have associated connector named as conditionname.connector. This connector
   * will hold the data from the previous phase. However it is possible that multiple conditions are
   * chained together. In this case we do not need to create individual connector for each of the child condition
   * but they should have connector same as parent condition.
   * @param conditionNodes Set of condition nodes in the spec
   * @param childToParent Map contains only conditions as keys. The corresponding value represents its immediate parent
   * @param conditionInputs Map of condition nodes to corresponding inputs
   * @param conditionOutputs Map of condition nodes to corresponding outputs
   * @param actionNodes Set of action nodes
   * @return the resolved connectors for each condition
   */
  private Map<String, String> getConnectorsAssociatedWithConditions(Set<String> conditionNodes,
                                                                    Map<String, String> childToParent,
                                                                    Map<String, Set<String>> conditionInputs,
                                                                    Map<String, Set<String>> conditionOutputs,
                                                                    Set<String> actionNodes) {
    Map<String, String> conditionConnectors = new HashMap<>();
    for (String condition : conditionNodes) {
      Set<String> inputs = conditionInputs.get(condition);
      Set<String> outputs = conditionOutputs.get(condition);

      if (actionNodes.containsAll(inputs) || actionNodes.containsAll(outputs)
        || conditionInputs.get(condition).isEmpty()) {
        // Put null connector for conditions which has all inputs or all outputs as action nodes
        // OR the condition for which there is no input connection
        conditionConnectors.put(condition, "null");
      } else {
        // Put the default connector for the condition
        conditionConnectors.put(condition, condition + ".connector");
      }
    }

    for (Map.Entry<String, String> entry : childToParent.entrySet()) {
      String parent = entry.getValue();
      while (parent != null) {
        conditionConnectors.put(entry.getKey(), conditionConnectors.get(parent));
        parent = childToParent.get(parent);
      }
    }

    conditionConnectors.values().removeAll(Collections.singleton("null"));
    return conditionConnectors;
  }

  /**
   * Update the current dag by replacing conditions in the dag with the corresponding condition connectors
   */
  private Dag getUpdatedDag(Dag dag, Map<String, String> controlConnectors) {
    Set<String> controlAsSources = Sets.intersection(controlConnectors.keySet(), dag.getSources());
    Set<String> controlAsSink =  Sets.intersection(controlConnectors.keySet(), dag.getSinks());
    if (controlAsSources.isEmpty() && controlAsSink.isEmpty()) {
      return dag;
    }

    Set<Connection> newConnections = new HashSet<>();

    for (String node : dag.getNodes()) {
      String newNode = controlConnectors.get(node) == null ? node : controlConnectors.get(node);
      for (String inputNode : dag.getNodeInputs(node)) {
        newConnections.add(new Connection(controlConnectors.get(inputNode) == null ? inputNode
                                            : controlConnectors.get(inputNode), newNode));
      }

      for (String outputNode : dag.getNodeOutputs(node)) {
        newConnections.add(new Connection(newNode, controlConnectors.get(outputNode) == null ? outputNode
          : controlConnectors.get(outputNode)));
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
        String connectorType = dag.getSources().contains(stageName) ?
          Constants.Connector.SOURCE_TYPE : Constants.Connector.SINK_TYPE;
        PluginSpec connectorSpec =
          new PluginSpec(Constants.Connector.PLUGIN_TYPE, "connector",
                         ImmutableMap.of(Constants.Connector.ORIGINAL_NAME, originalName != null
                                           ? originalName : stageName,
                                         Constants.Connector.TYPE, connectorType), null);
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
                        Set<String> isolationNodes, Set<String> actionNodes, Map<String, String> connectorNodes) {
    Dag dag = new Dag(connections);
    Set<Dag> subdags = dag.splitByControlNodes(conditions, actionNodes);

    Set<Dag> result = new HashSet<>();
    for (Dag subdag : subdags) {
      if (Sets.union(conditions, actionNodes).containsAll(subdag.getNodes())) {
        // Current Dag only contains control nodes and no reducers/isolation nodes. So no need to insert any
        // connectors here, which is only done for reducer/isolation nodes.
        result.add(subdag);
        continue;
      }
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
