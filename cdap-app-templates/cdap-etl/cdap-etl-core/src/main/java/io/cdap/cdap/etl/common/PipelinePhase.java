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

package io.cdap.cdap.etl.common;

import com.google.common.base.Joiner;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.etl.planner.Dag;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Keeps track of the plugin ids for the source, transforms, and sink of a pipeline phase.
 */
public class PipelinePhase implements Iterable<StageSpec>, Serializable {
  // plugin type -> stage info
  private final Map<String, Set<StageSpec>> stagesByType;
  private final Map<String, StageSpec> stagesByName;
  private final Dag dag;

  // dag is nullable in case the pipeline phase is a single stage (which is not a valid dag)
  private PipelinePhase(Set<StageSpec> stages, @Nullable Dag dag) {
    stagesByType = new HashMap<>();
    stagesByName = new HashMap<>();
    for (StageSpec stage : stages) {
      stagesByName.put(stage.getName(), stage);
      String pluginType = stage.getPluginType();
      Set<StageSpec> typeStages = stagesByType.get(pluginType);
      if (typeStages == null) {
        typeStages = new HashSet<>();
        stagesByType.put(pluginType, typeStages);
      }
      typeStages.add(stage);
    }
    this.dag = dag;
  }

  /**
   * Get an unmodifiable set of stages that use the specified plugin type.
   *
   * @param pluginType the plugin type
   * @return unmodifiable set of stages that use the specified plugin type
   */
  public Set<StageSpec> getStagesOfType(String pluginType) {
    Set<StageSpec> stagesOfType = stagesByType.get(pluginType);
    return Collections.unmodifiableSet(stagesOfType == null ? new HashSet<>() : stagesOfType);
  }

  public Set<StageSpec> getStagesOfType(String... pluginTypes) {
    Set<StageSpec> stageSpecs = new HashSet<>();
    for (String pluginType: pluginTypes) {
      Set<StageSpec> typeStages = stagesByType.get(pluginType);
      if (typeStages != null) {
        stageSpecs.addAll(typeStages);
      }
    }
    return Collections.unmodifiableSet(stageSpecs);
  }

  @Nullable
  public StageSpec getStage(String stageName) {
    return stagesByName.get(stageName);
  }

  public Set<String> getStageInputs(String stage) {
    Set<String> inputs = dag == null ? null : dag.getNodeInputs(stage);
    return Collections.unmodifiableSet(inputs == null ? new HashSet<>() : inputs);
  }

  public Set<String> getStageOutputs(String stage) {
    Set<String> outputs = dag == null ? null : dag.getNodeOutputs(stage);
    return Collections.unmodifiableSet(outputs == null ? new HashSet<>() : outputs);
  }

  public Set<String> getPluginTypes() {
    return stagesByType.keySet();
  }

  public Set<String> getSources() {
    // null dag means there's just one stage, which is both a source and a sink
    return dag == null ? Collections.singleton(stagesByName.values().iterator().next().getName()) : dag.getSources();
  }

  public Set<String> getSinks() {
    // null dag means there's just one stage, which is both a source and a sink
    return dag == null ? Collections.singleton(stagesByName.values().iterator().next().getName()) : dag.getSinks();
  }

  public int size() {
    return stagesByName.size();
  }

  @Nullable
  public Dag getDag() {
    return dag;
  }

  /**
   * Get a subset of the pipeline phase, starting from the sources and going to the specified nodes that will
   * be the new sinks of the pipeline subset.
   *
   * @param newSinks the new sinks to go to
   * @return subset of the pipeline, starting from current sources and going to the new sinks
   */
  public PipelinePhase subsetTo(Set<String> newSinks) {
    return getSubset(dag.subsetFrom(dag.getSources(), newSinks));
  }

  /**
   * Get a subset of the pipeline phase, starting from the specified new sources and going to the current sinks.
   *
   * @param newSources the new sources to start from
   * @return subset of the pipeline, starting from specified new sources and going to the current sinks
   */
  public PipelinePhase subsetFrom(Set<String> newSources) {
    return getSubset(dag.subsetFrom(newSources));
  }

  private PipelinePhase getSubset(final Dag subsetDag) {
    Set<StageSpec> subsetStages = new HashSet<>();
    for (StageSpec stage : stagesByName.values()) {
      if (subsetDag.getNodes().contains(stage.getName())) {
        subsetStages.add(stage);
      }
    }
    return new PipelinePhase(subsetStages, subsetDag);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PipelinePhase that = (PipelinePhase) o;

    return Objects.equals(stagesByType, that.stagesByType) &&
      Objects.equals(stagesByName, that.stagesByName) &&
      Objects.equals(dag, that.dag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stagesByType, stagesByName, dag);
  }

  @Override
  public String toString() {
    return "PipelinePhase{" +
      "stagesByType=" + stagesByType +
      ", stagesByName=" + stagesByName +
      ", dag=" + dag +
      '}';
  }

  /**
   * Registers all the plugin to the given pluginConfigurer by calling {@link PluginConfigurer#usePluginClass(String,
   * String, String, PluginProperties, PluginSelector)}
   *
   * @param pluginConfigurer the {@link PluginConfigurer} to which the plugins in this {@link PipelinePhase} needs to be
   * registered
   * @param runtimeConfigurer the runtime configurer to provide runtime arguments to resolve macro better, null
   *                          if this is the initial deploy
   * @param namespace namespace the app is deployed
   */
  public void registerPlugins(PluginConfigurer pluginConfigurer, @Nullable RuntimeConfigurer runtimeConfigurer,
                              String namespace) {
    MacroParserOptions options = MacroParserOptions.builder().skipInvalidMacros().setEscaping(false)
                                   .setFunctionWhitelist(ConnectionMacroEvaluator.FUNCTION_NAME).build();
    MacroEvaluator runtimeEvaluator = null;
    if (runtimeConfigurer != null) {
      Map<String, MacroEvaluator> evaluators = Collections.singletonMap(
        ConnectionMacroEvaluator.FUNCTION_NAME, new ConnectionMacroEvaluator(namespace, runtimeConfigurer));
      runtimeEvaluator = new DefaultMacroEvaluator(
        new BasicArguments(runtimeConfigurer.getRuntimeArguments()), evaluators, Collections.singleton(
        ConnectionMacroEvaluator.FUNCTION_NAME));
    }
    for (StageSpec stageSpec : stagesByName.values()) {
      // we don't need to register connectors only source, sink and transform plugins
      if (stageSpec.getPluginType().equals(Constants.Connector.PLUGIN_TYPE)) {
        continue;
      }
      PluginSpec pluginSpec = stageSpec.getPlugin();
      ArtifactVersion version = pluginSpec.getArtifact().getVersion();
      ArtifactSelector artifactSelector = new ArtifactSelector(pluginSpec.getArtifact().getScope(),
                                                               pluginSpec.getArtifact().getName(),
                                                               new ArtifactVersionRange(version, true, version, true));
      Map<String, String> prop = pluginSpec.getProperties();
      pluginConfigurer.usePluginClass(pluginSpec.getType(), pluginSpec.getName(), stageSpec.getName(),
                                      PluginProperties.builder().addAll(
                                        runtimeConfigurer == null ? prop :
                                          pluginConfigurer.evaluateMacros(prop, runtimeEvaluator, options)).build(),
                                      artifactSelector);
    }
  }

  /**
   * Get a builder used to create a pipeline phase.
   *
   * @param supportedPluginTypes types of plugins supported in the phase
   * @return builder used to create a pipeline phase
   */
  public static Builder builder(Set<String> supportedPluginTypes) {
    return new Builder(supportedPluginTypes);
  }

  @Override
  public Iterator<StageSpec> iterator() {
    return stagesByName.values().iterator();
  }

  /**
   * Builder to create a {@link PipelinePhase}.
   */
  public static class Builder {
    private final Set<String> supportedPluginTypes;
    private final Set<StageSpec> stages;
    private final Set<io.cdap.cdap.etl.proto.Connection> connections;

    public Builder(Set<String> supportedPluginTypes) {
      this.supportedPluginTypes = supportedPluginTypes;
      this.stages = new HashSet<>();
      this.connections = new HashSet<>();
    }

    public Builder addStage(StageSpec stageSpec) {
      String pluginType = stageSpec.getPluginType();
      if (!supportedPluginTypes.contains(pluginType)) {
        throw new IllegalArgumentException(
          String.format("%s is an unsupported plugin type. Plugin type must be one of %s.",
                        pluginType, Joiner.on(',').join(supportedPluginTypes)));
      }
      stages.add(stageSpec);
      return this;
    }

    public Builder addStages(Collection<StageSpec> stages) {
      for (StageSpec stage : stages) {
        addStage(stage);
      }
      return this;
    }

    public Builder addConnection(String from, String to) {
      connections.add(new io.cdap.cdap.etl.proto.Connection(from, to));
      return this;
    }

    public Builder addConnections(String input, Collection<String> outputs) {
      for (String output : outputs) {
        addConnection(input, output);
      }
      return this;
    }

    public Builder addConnections(Collection<io.cdap.cdap.etl.proto.Connection> connections) {
      this.connections.addAll(connections);
      return this;
    }

    public PipelinePhase build() {
      return new PipelinePhase(stages, connections.isEmpty() ? null : new Dag(connections));
    }
  }
}
