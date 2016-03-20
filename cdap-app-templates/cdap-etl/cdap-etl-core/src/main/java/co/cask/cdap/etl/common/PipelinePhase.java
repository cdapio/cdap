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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Keeps track of the plugin ids for the source, transforms, and sink of a pipeline phase.
 */
public class PipelinePhase {
  // plugin type -> stage info
  private final Map<String, Set<StageInfo>> stages;
  private final Map<String, Set<String>> connections;
  private final Set<String> sources;
  private final Set<String> sinks;

  private PipelinePhase(Map<String, Set<StageInfo>> stages, Map<String, Set<String>> connections) {
    this.stages = ImmutableMap.copyOf(stages);
    this.connections = ImmutableMap.copyOf(connections);
    Set<String> stagesWithOutput = new HashSet<>();
    for (Set<String> outputStages : connections.values()) {
      stagesWithOutput.addAll(outputStages);
    }
    this.sources = Sets.difference(connections.keySet(), stagesWithOutput);
    this.sinks = Sets.difference(stagesWithOutput, connections.keySet());
  }

  /**
   * Get an unmodifiable set of stages that use the specified plugin type.
   *
   * @param pluginType the plugin type
   * @return unmodifiable set of stages that use the specified plugin type
   */
  public Set<StageInfo> getStagesOfType(String pluginType) {
    Set<StageInfo> stageInfos = stages.get(pluginType);
    return Collections.unmodifiableSet(stageInfos == null ? new HashSet<StageInfo>() : stageInfos);
  }

  public Map<String, Set<String>> getConnections() {
    return connections;
  }

  public Set<String> getStageOutputs(String stage) {
    Set<String> outputs = connections.get(stage);
    return Collections.unmodifiableSet(outputs == null ? new HashSet<String>() : outputs);
  }

  public Set<String> getSources() {
    return sources;
  }

  public Set<String> getSinks() {
    return sinks;
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

    return Objects.equals(stages, that.stages) &&
      Objects.equals(connections, that.connections) &&
      Objects.equals(sources, that.sources) &&
      Objects.equals(sinks, that.sinks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stages, connections, sources, sinks);
  }

  @Override
  public String toString() {
    return "PipelinePhase{" +
      "stages=" + stages +
      ", connections=" + connections +
      ", sources=" + sources +
      ", sinks=" + sinks +
      '}';
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

  /**
   * Builder to create a {@link PipelinePhase}.
   */
  public static class Builder {
    private final Set<String> supportedPluginTypes;
    private final Map<String, Set<StageInfo>> stages;
    private final Map<String, Set<String>> connections;

    public Builder(Set<String> supportedPluginTypes) {
      this.supportedPluginTypes = supportedPluginTypes;
      this.stages = new HashMap<>();
      this.connections = new HashMap<>();
    }

    public Builder addStage(String pluginType, StageInfo stageInfo) {
      return addStages(pluginType, ImmutableSet.of(stageInfo));
    }

    public Builder addStages(String pluginType, Collection<StageInfo> stages) {
      if (!supportedPluginTypes.contains(pluginType)) {
        throw new IllegalArgumentException(
          String.format("%s is an unsupported plugin type. Plugin type must be one of %s.",
                        pluginType, Joiner.on(',').join(supportedPluginTypes)));
      }
      Set<StageInfo> existingStages = this.stages.get(pluginType);
      if (existingStages == null) {
        existingStages = new HashSet<>();
        this.stages.put(pluginType, existingStages);
      }
      existingStages.addAll(stages);
      return this;
    }

    public Builder addConnection(String from, String to) {
      return addConnections(from, ImmutableSet.of(to));
    }

    public Builder addConnections(String from, Collection<String> to) {
      Set<String> existingOutputs = connections.get(from);
      if (existingOutputs == null) {
        existingOutputs = new HashSet<>();
        connections.put(from, existingOutputs);
      }
      existingOutputs.addAll(to);
      return this;
    }

    public Builder addConnections(Map<String, Set<String>> connections) {
      this.connections.putAll(connections);
      return this;
    }

    public PipelinePhase build() {
      return new PipelinePhase(stages, connections);
    }
  }
}
