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

package co.cask.cdap.etl.proto.v1;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.UpgradeableConfig;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ETL Realtime Configuration. Public constructors are all deprecated in favor of the builder.
 */
public final class ETLRealtimeConfig extends ETLConfig
  implements UpgradeableConfig<co.cask.cdap.etl.proto.v2.ETLRealtimeConfig> {
  private final Integer instances;

  @Deprecated
  public ETLRealtimeConfig(Integer instances, ETLStage source, List<ETLStage> sinks,
                           List<ETLStage> transforms, List<Connection> connections, Resources resources) {
    super(source, sinks, transforms, connections, resources);
    this.instances = instances;
  }

  @Deprecated
  public ETLRealtimeConfig(ETLStage source, List<ETLStage> sinks,
                           List<ETLStage> transforms, List<Connection> connections) {
    this(1, source, sinks, transforms, connections, null);

  }

  @Deprecated
  public ETLRealtimeConfig(ETLStage source, ETLStage sinks,
                           List<ETLStage> transforms, List<Connection> connections) {
    this(1, source, sinks, transforms, connections, null);
  }

  @Deprecated
  public ETLRealtimeConfig(Integer instances, ETLStage source, ETLStage sink, List<ETLStage> transforms,
                           List<Connection> connections, Resources resources) {
    super(source, sink, transforms, connections, resources);
    this.instances = instances;
  }

  @Deprecated
  public ETLRealtimeConfig(int instances, ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this(instances, source, sink, transforms, new ArrayList<Connection>(), null);
  }

  @Deprecated
  @VisibleForTesting
  public ETLRealtimeConfig(ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this(1, source, sink, transforms);
  }

  @Deprecated
  @VisibleForTesting
  public ETLRealtimeConfig(ETLStage source, ETLStage sink) {
    this(source, sink, null);
  }

  public Integer getInstances() {
    return instances == null ? 1 : instances;
  }

  @Override
  public boolean canUpgrade() {
    return true;
  }

  @Override
  public co.cask.cdap.etl.proto.v2.ETLRealtimeConfig upgrade(UpgradeContext upgradeContext) {
    co.cask.cdap.etl.proto.v2.ETLRealtimeConfig.Builder builder =
      co.cask.cdap.etl.proto.v2.ETLRealtimeConfig.builder()
        .setInstances(getInstances());

    return upgradeBase(builder, upgradeContext, RealtimeSource.PLUGIN_TYPE, RealtimeSink.PLUGIN_TYPE).build();
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
      return new ETLRealtimeConfig(instances, source, sinks, transforms, connections, resources);
    }
  }
}
