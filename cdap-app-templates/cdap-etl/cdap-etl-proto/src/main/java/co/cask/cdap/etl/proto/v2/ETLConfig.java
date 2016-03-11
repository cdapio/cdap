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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Common ETL Config.
 */
// though not marked nullable, fields can be null since these objects are created through gson deserialization
@SuppressWarnings("ConstantConditions")
public class ETLConfig extends Config {
  private final List<ETLStage> stages;
  private final List<Connection> connections;
  private final Resources resources;
  private final Boolean stageLoggingEnabled;

  protected ETLConfig(List<ETLStage> stages, List<Connection> connections,
                      Resources resources, boolean stageLoggingEnabled) {
    this.stages = Collections.unmodifiableList(stages);
    this.connections = Collections.unmodifiableList(connections);
    this.resources = resources;
    this.stageLoggingEnabled = stageLoggingEnabled;
  }

  public List<ETLStage> getStages() {
    return stages == null ? Collections.unmodifiableList(new ArrayList<ETLStage>()) : stages;
  }

  public List<Connection> getConnections() {
    return connections == null ? Collections.unmodifiableList(new ArrayList<Connection>()) : connections;
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

  /**
   * Builder for creating configs.
   */
  @SuppressWarnings("unchecked")
  public abstract static class Builder<T extends Builder> {
    protected List<ETLStage> stages;
    protected List<Connection> connections;
    protected Resources resources;
    protected Boolean stageLoggingEnabled;

    protected Builder() {
      this.stages = new ArrayList<>();
      this.connections = new ArrayList<>();
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
