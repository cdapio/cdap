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

package co.cask.cdap.etl.proto.v2;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.UpgradeableConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Common ETL Config.
 */
// though not marked nullable, fields can be null since these objects are created through gson deserialization
@SuppressWarnings("ConstantConditions")
public class ETLConfig extends Config implements UpgradeableConfig {
  private final String description;
  private final Set<ETLStage> stages;
  private final Set<Connection> connections;
  private final Resources resources;
  private final Resources driverResources;
  private final Resources clientResources;
  private final Boolean stageLoggingEnabled;
  private final Boolean processTimingEnabled;
  private final Integer numOfRecordsPreview;
  private final Map<String, String> properties;
  // v1 fields to support backwards compatibility
  private final co.cask.cdap.etl.proto.v1.ETLStage source;
  private final List<co.cask.cdap.etl.proto.v1.ETLStage> sinks;
  private final List<co.cask.cdap.etl.proto.v1.ETLStage> transforms;

  protected ETLConfig(Set<ETLStage> stages, Set<Connection> connections,
                      Resources resources, Resources driverResources, Resources clientResources,
                      boolean stageLoggingEnabled, boolean processTimingEnabled,
                      int numOfRecordsPreview, Map<String, String> properties) {
    this.description = null;
    this.stages = Collections.unmodifiableSet(stages);
    this.connections = Collections.unmodifiableSet(connections);
    this.resources = resources;
    this.driverResources = driverResources;
    this.clientResources = clientResources;
    this.stageLoggingEnabled = stageLoggingEnabled;
    this.processTimingEnabled = processTimingEnabled;
    this.numOfRecordsPreview = numOfRecordsPreview;
    this.properties = Collections.unmodifiableMap(properties);
    // these fields are only here for backwards compatibility
    this.source = null;
    this.sinks = new ArrayList<>();
    this.transforms = new ArrayList<>();
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public Set<ETLStage> getStages() {
    return Collections.unmodifiableSet(stages == null ? new HashSet<ETLStage>() : stages);
  }

  public Set<Connection> getConnections() {
    return Collections.unmodifiableSet(connections == null ? new HashSet<Connection>() : connections);
  }

  public Resources getResources() {
    return resources == null ? new Resources(1024, 1) : resources;
  }

  public Resources getDriverResources() {
    return driverResources == null ? new Resources(1024, 1) : driverResources;
  }

  public Resources getClientResources() {
    return clientResources == null ? new Resources(1024, 1) : clientResources;
  }

  public int getNumOfRecordsPreview() {
    return numOfRecordsPreview == null ? 100 : numOfRecordsPreview;
  }

  public boolean isStageLoggingEnabled() {
    return stageLoggingEnabled == null ? true : stageLoggingEnabled;
  }

  public boolean isProcessTimingEnabled() {
    return processTimingEnabled == null ? true : processTimingEnabled;
  }

  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties == null ? new HashMap<String, String>() : properties);
  }

  /**
   * Validate correctness. Since this object is created through deserialization, some fields that should not be null
   * may be null. Only validates field correctness, not logical correctness.
   *
   * @throws IllegalArgumentException if the object is invalid
   */
  public void validate() {
    for (ETLStage stage : getStages()) {
      stage.validate();
    }
  }

  /**
   * Converts the source, transforms, sinks, connections and resources from old style config into their
   * new forms.
   */
  protected <T extends Builder> T convertStages(T builder, String sourceType, String sinkType) {
    builder
      .setResources(getResources())
      .addConnections(getConnections());
    if (!isStageLoggingEnabled()) {
      builder.disableStageLogging();
    }

    UpgradeContext dummyUpgradeContext = new UpgradeContext() {
      @Nullable
      @Override
      public ArtifactSelectorConfig getPluginArtifact(String pluginType, String pluginName) {
        return null;
      }
    };
    if (source == null) {
      throw new IllegalArgumentException("Pipeline does not contain a source.");
    }
    builder.addStage(source.upgradeStage(sourceType, dummyUpgradeContext));
    if (transforms != null) {
      for (co.cask.cdap.etl.proto.v1.ETLStage v1Stage : transforms) {
        builder.addStage(v1Stage.upgradeStage(Transform.PLUGIN_TYPE, dummyUpgradeContext));
      }
    }
    if (sinks == null || sinks.isEmpty()) {
      throw new IllegalArgumentException("Pipeline does not contain any sinks.");
    }
    for (co.cask.cdap.etl.proto.v1.ETLStage v1Stage : sinks) {
      builder.addStage(v1Stage.upgradeStage(sinkType, dummyUpgradeContext));
    }
    return builder;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ETLConfig that = (ETLConfig) o;

    return Objects.equals(description, that.description) &&
      Objects.equals(stages, that.stages) &&
      Objects.equals(connections, that.connections) &&
      Objects.equals(getResources(), that.getResources()) &&
      Objects.equals(getDriverResources(), that.getDriverResources()) &&
      Objects.equals(getClientResources(), that.getClientResources()) &&
      Objects.equals(getProperties(), that.getProperties()) &&
      isStageLoggingEnabled() == that.isStageLoggingEnabled() &&
      isProcessTimingEnabled() == that.isProcessTimingEnabled() &&
      getNumOfRecordsPreview() == that.getNumOfRecordsPreview();
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, stages, connections, getResources(), getDriverResources(), getClientResources(),
                        isStageLoggingEnabled(), isProcessTimingEnabled(), getNumOfRecordsPreview(), getProperties());
  }

  @Override
  public boolean canUpgrade() {
    return false;
  }

  @Override
  public UpgradeableConfig upgrade(UpgradeContext upgradeContext) {
    throw new UnsupportedOperationException("This is the latest config and cannot be upgraded.");
  }

  @Override
  public String toString() {
    return "ETLConfig{" +
      "description='" + description + '\'' +
      ", stages=" + stages +
      ", connections=" + connections +
      ", resources=" + resources +
      ", driverResources=" + driverResources +
      ", clientResources=" + clientResources +
      ", stageLoggingEnabled=" + stageLoggingEnabled +
      ", processTimingEnabled=" + processTimingEnabled +
      ", numOfRecordsPreview=" + numOfRecordsPreview +
      ", properties=" + properties +
      ", source=" + source +
      ", sinks=" + sinks +
      ", transforms=" + transforms +
      "} " + super.toString();
  }

  /**
   * Builder for creating configs.
   *
   * @param <T> The actual builder type
   */
  @SuppressWarnings("unchecked")
  public abstract static class Builder<T extends Builder> {
    protected Set<ETLStage> stages;
    protected Set<Connection> connections;
    protected Resources resources;
    protected Resources driverResources;
    protected Resources clientResources;
    protected boolean stageLoggingEnabled;
    protected boolean processTimingEnabled;
    protected int numOfRecordsPreview;
    protected Map<String, String> properties;

    protected Builder() {
      this.stages = new HashSet<>();
      this.connections = new HashSet<>();
      this.resources = new Resources(1024, 1);
      this.driverResources = new Resources(1024, 1);
      this.clientResources = new Resources(1024, 1);
      this.stageLoggingEnabled = true;
      this.processTimingEnabled = true;
      this.properties = new HashMap<>();
    }

    public T addStage(ETLStage stage) {
      stages.add(stage);
      return (T) this;
    }

    public T addConnection(String from, String to) {
      this.connections.add(new Connection(from, to));
      return (T) this;
    }

    public T addConnection(String from, String to, String port) {
      this.connections.add(new Connection(from, to, port));
      return (T) this;
    }

    public T addConnection(String from, String to, Boolean condition) {
      this.connections.add(new Connection(from, to, condition));
      return (T) this;
    }

    public T addConnection(Connection connection) {
      this.connections.add(connection);
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

    public T setNumOfRecordsPreview(int numOfRecordsPreview) {
      this.numOfRecordsPreview = numOfRecordsPreview;
      return (T) this;
    }

    public T disableStageLogging() {
      this.stageLoggingEnabled = false;
      return (T) this;
    }

    public T disableProcessTiming() {
      this.processTimingEnabled = false;
      return (T) this;
    }

    public T setProperties(Map<String, String> properties) {
      this.properties = new HashMap<>(properties);
      return (T) this;
    }
  }
}
