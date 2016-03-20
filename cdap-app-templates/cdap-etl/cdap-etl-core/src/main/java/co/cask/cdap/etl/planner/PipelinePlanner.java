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
import com.google.common.collect.ImmutableSet;
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
  private final Set<String> supportedPluginTypes;

  public PipelinePlanner(Set<String> supportedPluginTypes, Set<String> reduceTypes) {
    this.reduceTypes = ImmutableSet.copyOf(reduceTypes);
    this.supportedPluginTypes = ImmutableSet.copyOf(supportedPluginTypes);
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
    Map<String, StageSpec> specs = new HashMap<>();
    for (StageSpec stage : spec.getStages()) {
      if (reduceTypes.contains(stage.getPlugin().getType())) {
        reduceNodes.add(stage.getName());
      }
      specs.put(stage.getName(), stage);
    }

    // insert connector stages into the logical pipeline
    ConnectorDag cdag = new ConnectorDag(spec.getConnections(), reduceNodes);
    cdag.insertConnectors();
    Set<String> connectorNodes = cdag.getConnectors();

    // now split the logical pipeline into pipeline phases, using the connectors as split points
    Map<String, Dag> subdags = new HashMap<>();
    // assign some name to each subdag
    for (Dag subdag : cdag.splitOnConnectors()) {
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
    return new PipelinePlan(phases, phaseConnections);
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
        phaseBuilder.addStage(Constants.CONNECTOR_TYPE, new StageInfo(stageName, null));
        continue;
      }

      // add other plugin types
      StageSpec spec = specs.get(stageName);
      String pluginType = spec.getPlugin().getType();
      StageInfo stageInfo = new StageInfo(stageName, spec.getErrorDatasetName());
      phaseBuilder.addStage(pluginType, stageInfo);
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
