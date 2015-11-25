/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.tool.config;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;

import java.util.ArrayList;
import java.util.List;

/**
 * ETL Batch Configuration.
 */
public final class OldETLBatchConfig extends OldETLConfig {
  private final String schedule;
  private final List<OldETLStage> actions;

  public OldETLBatchConfig(String schedule, OldETLStage source, List<OldETLStage> sinks, List<OldETLStage> transforms,
                           Resources resources, List<OldETLStage> actions) {
    super(source, sinks, transforms, resources);
    this.schedule = schedule;
    this.actions = actions;
  }

  public ETLBatchConfig getNewConfig() {
    ETLConfig newConfig = super.getNewConfig();
    int actionId = 1;
    List<ETLStage> newActions = new ArrayList<>();
    if (actions != null) {
      for (OldETLStage oldAction : actions) {
        newActions.add(new ETLStage(
          oldAction.getName() + "." + actionId,
          new Plugin(oldAction.getName(), oldAction.getProperties()),
          oldAction.getErrorDatasetName()));
        actionId++;
      }
    }
    return new ETLBatchConfig(
      schedule,
      newConfig.getSource(),
      newConfig.getSinks(),
      newConfig.getTransforms(),
      newConfig.getConnections(),
      newConfig.getResources(),
      newActions);
  }
}
