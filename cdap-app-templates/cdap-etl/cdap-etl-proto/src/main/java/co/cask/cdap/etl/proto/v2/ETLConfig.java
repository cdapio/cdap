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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Common ETL Config.
 */
// though not marked nullable, fields can be null since these objects are created through gson deserialization
@SuppressWarnings("ConstantConditions")
public class ETLConfig extends Config implements UpgradeableConfig {
  private final Set<ETLStage> stages;
  private final Set<Connection> connections;
  private final Resources resources;
  private final Resources driverResources;
  private final Resources clientResources;
  private final Boolean stageLoggingEnabled;
  // v1 fields to support backwards compatibility
  private final co.cask.cdap.etl.proto.v1.ETLStage source;
  private final List<co.cask.cdap.etl.proto.v1.ETLStage> sinks;
  private final List<co.cask.cdap.etl.proto.v1.ETLStage> transforms;
  private final Integer numOfRecordsPreview;

  protected ETLConfig(Set<ETLStage> stages, Set<Connection> connections,
                      Resources resources, Resources driverResources, Resources clientResources,
                      boolean stageLoggingEnabled, int numOfRecordsPreview) {
    this.stages = Collections.unmodifiableSet(stages);
    this.connections = Collections.unmodifiableSet(connections);
    this.resources = resources;
    this.driverResources = driverResources;
    this.clientResources = clientResources;
    this.stageLoggingEnabled = stageLoggingEnabled;
    // these fields are only here for backwards compatibility
    this.source = null;
    this.sinks = new ArrayList<>();
    this.transforms = new ArrayList<>();
    this.numOfRecordsPreview = numOfRecordsPreview;
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
  public String toString() {
    return "ETLConfig{" +
      "stages=" + stages +
      ", connections=" + connections +
      ", resources=" + resources +
      ", driverResources=" + driverResources +
      ", clientResources=" + clientResources +
      ", stageLoggingEnabled=" + stageLoggingEnabled +
      ", source=" + source +
      ", sinks=" + sinks +
      ", transforms=" + transforms +
      ", numOfRecordsPreview=" + numOfRecordsPreview +
      "} " + super.toString();
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

    return Objects.equals(stages, that.stages) &&
      Objects.equals(connections, that.connections) &&
      Objects.equals(resources, that.resources) &&
      Objects.equals(driverResources, that.driverResources) &&
      Objects.equals(clientResources, that.clientResources) &&
      isStageLoggingEnabled() == that.isStageLoggingEnabled();
  }

  @Override
  public int hashCode() {
    return Objects.hash(stages, resources, isStageLoggingEnabled());
  }

  @Override
  public boolean canUpgrade() {
    return false;
  }

  @Override
  public UpgradeableConfig upgrade(UpgradeContext upgradeContext) {
    throw new UnsupportedOperationException("This is the latest config and cannot be upgraded.");
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
    protected Boolean stageLoggingEnabled;
    protected int numOfRecordsPreview;

    protected Builder() {
      this.stages = new HashSet<>();
      this.connections = new HashSet<>();
      this.resources = new Resources();
      this.stageLoggingEnabled = true;
    }

    public T addStage(ETLStage stage) {
      stages.add(stage);
      return (T) this;
    }

    public T addConnection(String from, String to) {
      this.connections.add(new Connection(from, to));
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
  }
}
