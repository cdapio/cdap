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

import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

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
      specs.put(stage.getName(), stage);
    }

    // Map to hold set of stages to which there is a connection from a action stage.
    SetMultimap<String, String> outgoingActionConnections = HashMultimap.create();
    // Map to hold set of stages from which there is a connection to action stage.
    SetMultimap<String, String> incomingActionConnections = HashMultimap.create();

    Set<Connection> connectionsWithoutAction = new HashSet<>();

    // Remove the connections to and from Action nodes in the pipeline in order to build the
    // ConnectorDag. Since Actions can only occur before sources or after sink nodes, the creation
    // of the ConnectorDag should not be affected after removal of connections involving action nodes.
    for (Connection connection : spec.getConnections()) {
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

    // insert connector stages into the logical pipeline
    ConnectorDag cdag = ConnectorDag.builder()
      .addConnections(connectionsWithoutAction)
      .addReduceNodes(reduceNodes)
      .addIsolationNodes(isolationNodes)
      .build();
    cdag.insertConnectors();
    Set<String> connectorNodes = cdag.getConnectors();

    // now split the logical pipeline into pipeline phases, using the connectors as split points
    Map<String, Dag> subdags = new HashMap<>();
    // assign some name to each subdag
    for (Dag subdag : cdag.split()) {
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
          phaseConnections.add(new Connection(dag1Name, dag2Name));
        }
      }
    }

    // convert to objects the programs expect.
    Map<String, PipelinePhase> phases = new HashMap<>();
    for (Map.Entry<String, Dag> dagEntry : subdags.entrySet()) {
      phases.put(dagEntry.getKey(), dagToPipeline(dagEntry.getValue(), connectorNodes, specs));
    }

    populateActionPhases(specs, actionNodes, phases, phaseConnections, outgoingActionConnections,
                         incomingActionConnections, subdags);

    return new PipelinePlan(phases, phaseConnections);
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
   * Converts a Dag into a PipelinePhase, using what we know about the plugin type of each node in the dag.
   * The PipelinePhase is what programs will take as input, and keeps track of sources, transforms, sinks, etc.
   *
   * @param dag the dag to convert
   * @param connectors connector nodes across all dags
   * @param specs specifications for every stage
   * @return the converted dag
   */
  private PipelinePhase dagToPipeline(Dag dag, Set<String> connectors, Map<String, StageSpec> specs) {
    PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(supportedPluginTypes);

    for (String stageName : dag.getTopologicalOrder()) {
      Set<String> outputs = dag.getNodeOutputs(stageName);
      if (!outputs.isEmpty()) {
        phaseBuilder.addConnections(stageName, outputs);
      }

      // add connectors
      if (connectors.contains(stageName)) {
        phaseBuilder.addStage(StageSpec.builder(stageName, Constants.CONNECTOR_SPEC).build());
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
}
