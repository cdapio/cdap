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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.UpgradeableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ETL Batch Configuration.
 */
public final class ETLBatchConfig extends ETLConfig
  implements UpgradeableConfig<co.cask.cdap.etl.proto.v1.ETLBatchConfig> {
  private final String schedule;
  private final List<ETLStage> actions;

  public ETLBatchConfig(String schedule, ETLStage source, List<ETLStage> sinks, List<ETLStage> transforms,
                        Resources resources, List<ETLStage> actions) {
    super(source, sinks, transforms, resources);
    this.schedule = schedule;
    this.actions = actions;
  }

  public List<ETLStage> getActions() {
    return Collections.unmodifiableList(actions == null ? new ArrayList<ETLStage>() : actions);
  }

  @Override
  public boolean canUpgrade() {
    return true;
  }

  @Override
  public co.cask.cdap.etl.proto.v1.ETLBatchConfig upgrade(UpgradeContext upgradeContext) {
    co.cask.cdap.etl.proto.v1.ETLBatchConfig.Builder builder =
      co.cask.cdap.etl.proto.v1.ETLBatchConfig.builder(schedule)
        .setEngine(co.cask.cdap.etl.proto.v1.ETLBatchConfig.Engine.MAPREDUCE)
        .setDriverResources(getResources());

    upgradeBase(builder, upgradeContext, BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE);

    int actionNum = 1;
    for (ETLStage v0Action : getActions()) {
      builder.addAction(v0Action.upgradeStage(v0Action.getName() + "." + actionNum, "action", upgradeContext));
      actionNum++;
    }

    return builder.build();
  }
}
