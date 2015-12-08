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
import co.cask.cdap.etl.common.ETLConfig;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;

import java.util.List;

/**
 * ETL Realtime Configuration.
 */
public final class OldETLRealtimeConfig extends OldETLConfig {
  private final Integer instances;

  public OldETLRealtimeConfig(Integer instances, OldETLStage source, List<OldETLStage> sinks,
                              List<OldETLStage> transforms, Resources resources) {
    super(source, sinks, transforms, resources);
    this.instances = instances;
  }

  public ETLRealtimeConfig getNewConfig() {
    ETLConfig newConfig = super.getNewConfig();
    return new ETLRealtimeConfig(
      instances,
      newConfig.getSource(),
      newConfig.getSinks(),
      newConfig.getTransforms(),
      newConfig.getConnections(),
      newConfig.getResources());
  }
}
