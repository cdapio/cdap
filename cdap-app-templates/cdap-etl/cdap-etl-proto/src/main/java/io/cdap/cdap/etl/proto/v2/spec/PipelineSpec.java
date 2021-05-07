/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.proto.v2.spec;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.v2.ETLConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Specification for a pipeline. The application should get this from the PipelineSpecGenerator in order
 * to ensure that the spec is validated and created correctly.
 *
 * This is like an {@link ETLConfig} but its stages contain additional information calculated at configure time of
 * the application, like input and output schemas of each stage and the artifact selected for each plugin.
 */
public class PipelineSpec {
  private final Set<StageSpec> stages;
  private final Set<Connection> connections;
  private final Resources resources;
  private final Resources driverResources;
  private final Resources clientResources;
  private final boolean stageLoggingEnabled;
  private final boolean processTimingEnabled;
  private final int numOfRecordsPreview;
  private final Map<String, String> properties;

  // we don't need this config after deploy time so don't serialize it
  private final transient Set<String> connectionsUsed;

  protected PipelineSpec(Set<StageSpec> stages,
                         Set<Connection> connections,
                         Resources resources,
                         Resources driverResources,
                         Resources clientResources,
                         boolean stageLoggingEnabled,
                         boolean processTimingEnabled,
                         int numOfRecordsPreview,
                         Map<String, String> properties,
                         Set<String> connectionsUsed) {
    this.stages = ImmutableSet.copyOf(stages);
    this.connections = ImmutableSet.copyOf(connections);
    this.resources = resources;
    this.driverResources = driverResources;
    this.clientResources = clientResources;
    this.stageLoggingEnabled = stageLoggingEnabled;
    this.processTimingEnabled = processTimingEnabled;
    this.numOfRecordsPreview = numOfRecordsPreview;
    this.properties = ImmutableMap.copyOf(properties);
    this.connectionsUsed = connectionsUsed;
  }

  public Set<StageSpec> getStages() {
    return stages;
  }

  public Set<Connection> getConnections() {
    return connections;
  }

  public Resources getResources() {
    return resources;
  }

  public Resources getDriverResources() {
    return driverResources;
  }

  public Resources getClientResources() {
    return clientResources;
  }

  public boolean isStageLoggingEnabled() {
    return stageLoggingEnabled;
  }

  public boolean isProcessTimingEnabled() {
    return processTimingEnabled;
  }

  public int getNumOfRecordsPreview() {
    return numOfRecordsPreview;
  }

  public Set<String> getConnectionsUsed() {
    return connectionsUsed;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PipelineSpec that = (PipelineSpec) o;

    return Objects.equals(stages, that.stages) &&
      Objects.equals(connections, that.connections) &&
      Objects.equals(resources, that.resources) &&
      Objects.equals(driverResources, that.driverResources) &&
      Objects.equals(clientResources, that.clientResources) &&
      Objects.equals(properties, that.properties) &&
      stageLoggingEnabled == that.stageLoggingEnabled &&
      processTimingEnabled == that.processTimingEnabled &&
      numOfRecordsPreview == that.numOfRecordsPreview;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stages, connections, resources, driverResources, clientResources,
                        stageLoggingEnabled, processTimingEnabled, numOfRecordsPreview, properties);
  }

  @Override
  public String toString() {
    return "PipelineSpec{" +
      "stages=" + stages +
      ", connections=" + connections +
      ", resources=" + resources +
      ", driverResources=" + driverResources +
      ", clientResources=" + clientResources +
      ", stageLoggingEnabled=" + stageLoggingEnabled +
      ", processTimingEnabled=" + processTimingEnabled +
      ", numOfRecordsPreview=" + numOfRecordsPreview +
      ", properties=" + properties +
      "}";
  }

  /**
   * @return builder to create a spec.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Base builder for creating pipeline specs.
   *
   * @param <T> the actual builder type
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T extends Builder> {
    protected Set<StageSpec> stages;
    protected Set<Connection> connections;
    protected Resources resources;
    protected Resources driverResources;
    protected Resources clientResources;
    protected boolean stageLoggingEnabled;
    protected boolean processTimingEnabled;
    protected int numOfRecordsPreview;
    protected Map<String, String> properties;
    protected Set<String> connectionsUsed;

    protected Builder() {
      this.stages = new HashSet<>();
      this.connections = new HashSet<>();
      this.resources = new Resources();
      this.stageLoggingEnabled = true;
      this.processTimingEnabled = true;
      this.properties = new HashMap<>();
      this.connectionsUsed = new HashSet<>();
    }

    public T addStage(StageSpec stage) {
      stages.add(stage);
      return (T) this;
    }

    public T addStages(Collection<StageSpec> stages) {
      this.stages.addAll(stages);
      return (T) this;
    }

    public T addConnection(String from, String to) {
      connections.add(new Connection(from, to));
      return (T) this;
    }

    public T addConnections(Collection<Connection> connections) {
      this.connections.addAll(connections);
      return (T) this;
    }

    public T setResources(Resources resources) {
      this.resources = resources;
      return (T) this;
    }

    public T setDriverResources(Resources resources) {
      this.driverResources = resources;
      return (T) this;
    }

    public T setClientResources(Resources resources) {
      this.clientResources = resources;
      return (T) this;
    }

    public T setStageLoggingEnabled(boolean stageLoggingEnabled) {
      this.stageLoggingEnabled = stageLoggingEnabled;
      return (T) this;
    }

    public T setProcessTimingEnabled(boolean processTimingEnabled) {
      this.processTimingEnabled = processTimingEnabled;
      return (T) this;
    }

    public T setNumOfRecordsPreview(int numOfRecordsPreview) {
      this.numOfRecordsPreview = numOfRecordsPreview;
      return (T) this;
    }

    public T setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return (T) this;
    }

    public T addConnectionUsed(String connection) {
      this.connectionsUsed.add(connection);
      return (T) this;
    }

    public T addConnectionsUsed(Set<String> connectionsUsed) {
      this.connectionsUsed.addAll(connectionsUsed);
      return (T) this;
    }

    public PipelineSpec build() {
      return new PipelineSpec(stages, connections, resources, driverResources, clientResources,
                              stageLoggingEnabled, processTimingEnabled, numOfRecordsPreview, properties,
                              connectionsUsed);
    }
  }
}
