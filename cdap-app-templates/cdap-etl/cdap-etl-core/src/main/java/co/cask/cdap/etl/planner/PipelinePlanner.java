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

import java.util.Arrays;
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
    Set<Dag> splittedDags = split(spec.getConnections(), conditionBranches.keySet(), reduceNodes, isolationNodes,
                                  actionNodes, connectorNodes);

    // convert to objects the programs expect.
    Map<String, PipelinePhase> phases = new HashMap<>();

    // now split the logical pipeline into pipeline phases, using the connectors as split points
    Map<String, Dag> subdags = new HashMap<>();
    // assign some name to each subdag
    for (Dag subdag : splittedDags) {
      String name = getPhaseName(subdag.getSources(), subdag.getSinks());
      subdags.put(name, subdag);
      // create control phases (Condition and Action) if there are any
      addControlPhasesIfRequired(controlNodes, subdag, specs, phases);
    }

    // Maintain the control nodes for which phase connections have been added.
    // Since we include phase connection based on the common sinks between the two dags,
    // it is possible that we do not get chance to process some control phases.
    // For example if dag1 is Action1->someNode-->Condition and dag2 is Condition->Action2.
    // We will process Condition here, however phase connections for Action1 and Action2 will
    // be added later.
    Set<String> processedControlNodes = new HashSet<>();

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

          // check if common node is control node
          Set<String> controlAsSink = Sets.intersection(dag1.getSinks(), controlNodes);
          if (controlAsSink.isEmpty()) {
            // Common node is not control add connection between dag1Name and dag2Name.
            phaseConnections.add(new Connection(dag1Name, dag2Name));
            continue;
          }

          String controlNodeName = controlAsSink.iterator().next();
          // Add current control node to the processed set
          processedControlNodes.add(controlNodeName);

          if (!controlNodes.containsAll(dag1.getNodes())) {
            // Only add connection from dag 1 to the current control stage,
            // if dag 1 contains stages other than control stages. If dag 1 only contains
            // the control stages, then instead of using dagName as source of connection
            // we would need the name of the control stage in dag1's sources. For example consider
            // the following dags -
            //
            //  dag1: condition.to.action with nodes as condition--->action
            //  dag2: action.to.somenode with nodes as action--->somenode
            //
            // Currently we are processing common nodes between dag1 sink and dag2 source which is action
            // Now we cannot add connection from condition.to.action to action, since the dag 1 only contains
            // control nodes, it would get filtered out later. We need connection of the form condition->action.
            // This connection gets added when for some dag0: file->csv-sink->condition we process the
            // common condition node.
            phaseConnections.add(new Connection(dag1Name, controlNodeName));
          }

          ConditionBranches branches = conditionBranches.get(controlNodeName);
          Boolean condition = branches == null ?  null : dag2.getNodes().contains(branches.getTrueOutput());
          // check if dag 2 only contains control stages. if so dag1 sink(which is a control node) will
          // connect to dag2 sink(which is another control node) dag1 sink is same as dag 2 source
          // which is control node, otherwise current control node will connect to whole dag2Name
          String connectTo = controlNodes.containsAll(dag2.getNodes()) ? dag2.getSinks().iterator().next() : dag2Name;
          phaseConnections.add(new Connection(controlNodeName, connectTo, condition));

        } else if (Sets.intersection(dag1.getSources(), dag2.getSources()).size() > 0) {
          // Two subdags have common sources
          // Consider the scenario below
          //            |----Action2
          // Action1--->|
          //            |----Action3
          // In this case the splits would be (Action1, Action2) and (Action1, Action3).
          // There wont be any common sinks however sources would be common. We need to process
          // that as well. Logic is similar to above if block except it operates on the sources now.

          Set<String> controlAsSource = Sets.intersection(dag1.getSources(), controlNodes);

          if (controlAsSource.isEmpty()) {
            phaseConnections.add(new Connection(dag1Name, dag2Name));
            continue;
          }

          String controlNodeName = controlAsSource.iterator().next();
          processedControlNodes.add(controlNodeName);

          ConditionBranches branches = conditionBranches.get(controlNodeName);
          Boolean condition = branches == null ?  null : dag2.getNodes().contains(branches.getTrueOutput());
          // check if dag 2 only contains control stages. if so dag1 sink(which is a control node) will
          // connect to dag2 sink(which is another control node) dag1 sink is same as dag 2 source
          // which is control node, otherwise current control node will connect to whole dag2Name
          String connectTo = controlNodes.containsAll(dag2.getNodes()) ? dag2.getSinks().iterator().next() : dag2Name;
          phaseConnections.add(new Connection(controlNodeName, connectTo, condition));
        }
      }
    }

    Map<String, String> controlConnectors = getConnectorsAssociatedWithConditions(conditionBranches.keySet(),
                                                                                  conditionChildToParent,
                                                                                  conditionInputs, conditionOutputs,
                                                                                  actionNodes);

    // At this point we have added phase connections between subdags, processing control nodes if they are
    // common to the two dags as either source or sink. However subdag can only contain control nodes
    // when two control nodes are chained together for example Action-->Condition. We need to add connection
    // between them. Also we need to process any control nodes which are NOT yet in the processedControlNodes set.
    // For example if we have two splitted dags
    // dag1: Action-->Condition
    // dag2: Condition->N0
    // Now we processed connections from Condition, since its common between dag 1 sink and dag 2 source.
    // However we have not process Action yet, since there is not dag0 with which it is common. So the Action
    // will not be in processedControlNode set.
    for (Map.Entry<String, Dag> dagEntry : subdags.entrySet()) {
      Dag dag = dagEntry.getValue();
      if (controlNodes.containsAll(dag.getSources())) {
        String source = dag.getSources().iterator().next();
        String sink = dagEntry.getKey();
        if (controlNodes.containsAll(dag.getNodes())) {
          // Current dag only contain control nodes.
          sink = dag.getSinks().iterator().next();
        }
        if (!processedControlNodes.contains(source)) {

          // This control node is not processed yet, because it is first in the pipeline.
          ConditionBranches branches = conditionBranches.get(source);
          Boolean condition = branches == null ? null : dag.getNodes().contains(branches.getTrueOutput());
          phaseConnections.add(new Connection(source, sink, condition));
          processedControlNodes.add(source);
        }
      }

      if (controlNodes.containsAll(dag.getSinks())) {
        String source = dagEntry.getKey();
        if (controlNodes.containsAll(dag.getNodes())) {
          source = dag.getSources().iterator().next();
        }
        String sink = dag.getSinks().iterator().next();
        if (!processedControlNodes.contains(sink)) {
          // This control node is not processed yet, because it is last in the pipeline.
          // Conditions cannot be last however action nodes can be
          phaseConnections.add(new Connection(source, sink));
          processedControlNodes.add(sink);
        }
      }

      // If dag only contains control nodes then ignore it, since all of its connections are processed already
      if (controlNodes.containsAll(dag.getNodes())) {
        continue;
      }

      Dag updatedDag = getUpdatedDag(dag, controlConnectors);
      // Remove any control nodes from this dag
      if (!Sets.intersection(updatedDag.getNodes(), controlNodes).isEmpty()) {
        Set<String> nodes = Sets.difference(updatedDag.getNodes(), controlNodes);
        updatedDag = updatedDag.createSubDag(nodes);
      }

      phases.put(dagEntry.getKey(), dagToPipeline(updatedDag, connectorNodes, specs, controlConnectors));
    }

    return new PipelinePlan(phases, phaseConnections);
  }

  /**
   * This method is responsible for creating control(Condition and Action) phases if the stages are present in the Dag.
   */
  private void addControlPhasesIfRequired(Set<String> controlNodes, Dag dag, Map<String, StageSpec> stageSpecs,
                                          Map<String, PipelinePhase> phases) {
    // Add control phases corresponding to the subdag source
    if (controlNodes.containsAll(dag.getSources())) {
      if (dag.getSources().size() != 1) {
        // sources should only have a single stage if its control
        throw new IllegalStateException(String.format("Dag '%s' to '%s' cannot have multiple sources when one of the" +
                                                        "source is a control (Condition or Action).", dag.getSources(),
                                                      dag.getSinks()));
      }

      String controlNode = dag.getSources().iterator().next();
      if (!phases.containsKey(controlNode)) {
        PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);
        PipelinePhase controlPhase = phaseBuilder
          .addStage(stageSpecs.get(controlNode))
          .build();
        phases.put(controlNode, controlPhase);
      }
    }

    // Add control phases corresponding to the subdag sink
    if (controlNodes.containsAll(dag.getSinks())) {
      if (dag.getSinks().size() != 1) {
        // sinks should only have a single stage if its control
        throw new IllegalStateException(String.format("Dag '%s' to '%s' cannot have multiple sinks when one of the" +
                                                        "sink is a control (Condition or Action).", dag.getSources(),
                                                      dag.getSinks()));
      }

      String controlNode = dag.getSinks().iterator().next();
      if (!phases.containsKey(controlNode)) {
        PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);
        PipelinePhase controlPhase = phaseBuilder
          .addStage(stageSpecs.get(controlNode))
          .build();
        phases.put(controlNode, controlPhase);
      }
    }
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
    Set<Dag> subdags = dag.splitByControlNodes(Sets.union(conditions, actionNodes));

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
