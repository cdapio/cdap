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
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.UpgradeableConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Common ETL Config.
 */
// though not marked nullable, fields can be null since these objects are created through gson deserialization
@SuppressWarnings("ConstantConditions")
public class ETLConfig extends Config implements UpgradeableConfig {
  private final Set<ETLStage> stages;
  private final Set<Connection> connections;
  private final Resources resources;
  private final Boolean stageLoggingEnabled;

  protected ETLConfig(Set<ETLStage> stages, Set<Connection> connections,
                      Resources resources, boolean stageLoggingEnabled) {
    this.stages = Collections.unmodifiableSet(stages);
    this.connections = Collections.unmodifiableSet(connections);
    this.resources = resources;
    this.stageLoggingEnabled = stageLoggingEnabled;
  }

  public Set<ETLStage> getStages() {
    return Collections.unmodifiableSet(stages == null ? new HashSet<ETLStage>() : stages);
  }

  public Set<Connection> getConnections() {
    return Collections.unmodifiableSet(connections == null ? new HashSet<Connection>() : connections);
  }

  public Resources getResources() {
    return resources == null ? new Resources() : resources;
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
    for (ETLStage stage : stages) {
      stage.validate();
    }
  }

  @Override
  public String toString() {
    return "ETLConfig{" +
      "stageLoggingEnabled=" + stageLoggingEnabled +
      ", stages=" + stages +
      ", resources=" + resources +
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
      Objects.equals(resources, that.resources) &&
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
   */
  @SuppressWarnings("unchecked")
  public abstract static class Builder<T extends Builder> {
    protected Set<ETLStage> stages;
    protected Set<Connection> connections;
    protected Resources resources;
    protected Boolean stageLoggingEnabled;

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

    public T disableStageLogging() {
      this.stageLoggingEnabled = false;
      return (T) this;
    }
  }
}
