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

package co.cask.cdap.etl.proto.v0;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.UpgradeableConfig;

import java.util.List;

/**
 * ETL Realtime Configuration.
 */
public final class ETLRealtimeConfig extends ETLConfig
  implements UpgradeableConfig<co.cask.cdap.etl.proto.v1.ETLRealtimeConfig> {
  private final Integer instances;

  public ETLRealtimeConfig(Integer instances, ETLStage source, List<ETLStage> sinks,
                           List<ETLStage> transforms, Resources resources) {
    super(source, sinks, transforms, resources);
    this.instances = instances;
  }

  public int getInstances() {
    return instances == null ? 1 : instances;
  }

  @Override
  public boolean canUpgrade() {
    return true;
  }

  @Override
  public co.cask.cdap.etl.proto.v1.ETLRealtimeConfig upgrade(UpgradeContext upgradeContext) {
    co.cask.cdap.etl.proto.v1.ETLRealtimeConfig.Builder builder = co.cask.cdap.etl.proto.v1.ETLRealtimeConfig.builder()
      .setInstances(getInstances());

    return upgradeBase(builder, upgradeContext, BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE).build();
  }
}
