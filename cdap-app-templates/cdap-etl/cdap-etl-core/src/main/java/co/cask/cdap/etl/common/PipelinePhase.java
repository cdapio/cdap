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

import co.cask.cdap.etl.planner.Dag;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.proto.Connection;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Keeps track of the plugin ids for the source, transforms, and sink of a pipeline phase.
 */
public class PipelinePhase implements Iterable<StageInfo> {
  // plugin type -> stage info
  private final Map<String, Set<StageInfo>> stages;
  private final Dag dag;

  private PipelinePhase(Map<String, Set<StageInfo>> stages, Dag dag) {
    this.stages = ImmutableMap.copyOf(stages);
    this.dag = dag;
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

  public Set<String> getStageOutputs(String stage) {
    Set<String> outputs = dag.getNodeOutputs(stage);
    return Collections.unmodifiableSet(outputs == null ? new HashSet<String>() : outputs);
  }

  public Set<String> getPluginTypes() {
    return stages.keySet();
  }

  public Set<String> getSources() {
    return dag.getSources();
  }

  public Set<String> getSinks() {
    return dag.getSinks();
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
    Map<String, Set<StageInfo>> subsetStages = new HashMap<>();
    for (Map.Entry<String, Set<StageInfo>> stagesEntry : stages.entrySet()) {
      final Set<StageInfo> stagesOfType = Sets.filter(stagesEntry.getValue(), new Predicate<StageInfo>() {
        @Override
        public boolean apply(StageInfo stageInfo) {
          return subsetDag.getNodes().contains(stageInfo.getName());
        }
      });
      if (!stagesOfType.isEmpty()) {
        subsetStages.put(stagesEntry.getKey(), stagesOfType);
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

    return Objects.equals(stages, that.stages) &&
      Objects.equals(dag, that.dag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stages, dag);
  }

  @Override
  public String toString() {
    return "PipelinePhase{" +
      "stages=" + stages +
      ", dag=" + dag +
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

  @Override
  public Iterator<StageInfo> iterator() {
    List<Iterator<StageInfo>> iterators = new ArrayList<>(stages.size());
    for (Map.Entry<String, Set<StageInfo>> stagesEntry : stages.entrySet()) {
      iterators.add(stagesEntry.getValue().iterator());
    }
    return Iterators.concat(iterators.iterator());
  }

  /**
   * Builder to create a {@link PipelinePhase}.
   */
  public static class Builder {
    private final Set<String> supportedPluginTypes;
    private final Map<String, Set<StageInfo>> stages;
    private final Set<co.cask.cdap.etl.proto.Connection> connections;

    public Builder(Set<String> supportedPluginTypes) {
      this.supportedPluginTypes = supportedPluginTypes;
      this.stages = new HashMap<>();
      this.connections = new HashSet<>();
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
      for (String toStage : to) {
        connections.add(new Connection(from, toStage));
      }
      return this;
    }

    public Builder addConnections(Map<String, Set<String>> connections) {
      for (Map.Entry<String, Set<String>> entry : connections.entrySet()) {
        addConnections(entry.getKey(), entry.getValue());
      }
      return this;
    }

    public PipelinePhase build() {
      return new PipelinePhase(stages, new Dag(connections));
    }
  }
}
