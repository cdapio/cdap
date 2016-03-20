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

import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.proto.Connection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Plan for a logical pipeline.
 */
public class PipelinePlan {
  private final Map<String, PipelinePhase> phases;
  private final Set<Connection> phaseConnections;

  public PipelinePlan(Map<String, PipelinePhase> phases, Collection<Connection> phaseConnections) {
    this.phases = ImmutableMap.copyOf(phases);
    this.phaseConnections = ImmutableSet.copyOf(phaseConnections);
  }

  public Map<String, PipelinePhase> getPhases() {
    return phases;
  }

  public Set<Connection> getPhaseConnections() {
    return phaseConnections;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PipelinePlan that = (PipelinePlan) o;

    return Objects.equals(phases, that.phases) &&
      Objects.equals(phaseConnections, that.phaseConnections);
  }

  @Override
  public int hashCode() {
    return Objects.hash(phases, phaseConnections);
  }

  @Override
  public String toString() {
    return "PipelinePlan{" +
      "phases=" + phases +
      ", phaseConnections=" + phaseConnections +
      '}';
  }
}
