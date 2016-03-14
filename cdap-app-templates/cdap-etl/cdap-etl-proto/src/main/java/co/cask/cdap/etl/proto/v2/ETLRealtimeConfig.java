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

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.proto.Connection;

import java.util.Objects;
import java.util.Set;

/**
 * ETL Realtime Configuration.
 */
public final class ETLRealtimeConfig extends ETLConfig {
  private final Integer instances;

  private ETLRealtimeConfig(Set<ETLStage> stages,
                            Set<Connection> connections,
                            Resources resources,
                            boolean stageLoggingEnabled,
                            int instances) {
    super(stages, connections, resources, stageLoggingEnabled);
    this.instances = instances;
  }

  public int getInstances() {
    return instances == null ? 1 : instances;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ETLRealtimeConfig that = (ETLRealtimeConfig) o;

    return Objects.equals(instances, that.instances);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), instances);
  }

  @Override
  public String toString() {
    return "ETLRealtimeConfig{" +
      "instances=" + instances +
      "} " + super.toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to create realtime etl configs.
   */
  public static class Builder extends ETLConfig.Builder<Builder> {
    private int instances;

    public Builder() {
      this.instances = 1;
    }

    public Builder setInstances(int instances) {
      this.instances = instances;
      return this;
    }

    public ETLRealtimeConfig build() {
      return new ETLRealtimeConfig(stages, connections, resources, stageLoggingEnabled, instances);
    }
  }
}
