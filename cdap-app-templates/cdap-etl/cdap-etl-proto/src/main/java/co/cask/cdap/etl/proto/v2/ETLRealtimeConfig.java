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
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
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
                            int instances,
                            int numOfRecordsPreview) {
    super(stages, connections, resources, resources, resources, stageLoggingEnabled, numOfRecordsPreview);
    this.instances = instances;
  }

  /**
   * If this has the old v1 fields (source, sinks, transforms), convert them all to stages and create a proper
   * v2 config. If this is already a v2 config, just returns itself.
   * This method is only here to support backwards compatibility.
   *
   * @return A v2 config.
   */
  public ETLRealtimeConfig convertOldConfig() {
    if (!getStages().isEmpty()) {
      return this;
    }

    ETLRealtimeConfig.Builder builder = builder()
      .setInstances(getInstances());
    return convertStages(builder, RealtimeSource.PLUGIN_TYPE, RealtimeSink.PLUGIN_TYPE).build();
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
      return new ETLRealtimeConfig(stages, connections, resources, stageLoggingEnabled, instances, numOfRecordsPreview);
    }
  }
}
