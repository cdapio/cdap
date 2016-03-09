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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Keeps track of the plugin ids for the source, transforms, and sink of a pipeline phase.
 */
public class PipelinePhase {
  private final StageInfo source;
  private final StageInfo aggregator;
  private final Set<StageInfo> sinks;
  private final Set<StageInfo> transforms;
  private final Map<String, Set<String>> connections;

  public PipelinePhase(StageInfo source, StageInfo aggregator, Set<StageInfo> sinks, Set<StageInfo> transforms,
                       Map<String, Set<String>> connections) {
    this.source = source;
    this.aggregator = aggregator;
    this.sinks = ImmutableSet.copyOf(sinks);
    this.transforms = ImmutableSet.copyOf(transforms);
    this.connections = ImmutableMap.copyOf(connections);
  }

  public StageInfo getSource() {
    return source;
  }

  public StageInfo getAggregator() {
    return aggregator;
  }

  public Set<StageInfo> getSinks() {
    return sinks;
  }

  public Set<StageInfo> getTransforms() {
    return transforms;
  }

  public Map<String, Set<String>> getConnections() {
    return connections;
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

    return Objects.equals(source, that.source) &&
      Objects.equals(aggregator, that.aggregator) &&
      Objects.equals(sinks, that.sinks) &&
      Objects.equals(transforms, that.transforms) &&
      Objects.equals(connections, that.connections);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, aggregator, sinks, transforms, connections);
  }

  @Override
  public String toString() {
    return "PipelinePhase{" +
      "source=" + source +
      ", aggregator=" + aggregator +
      ", sinks=" + sinks +
      ", transforms=" + transforms +
      ", connections=" + connections +
      '}';
  }
}
