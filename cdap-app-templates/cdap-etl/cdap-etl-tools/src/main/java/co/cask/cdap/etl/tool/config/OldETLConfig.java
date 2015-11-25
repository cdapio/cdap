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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;

import java.util.ArrayList;
import java.util.List;

/**
 * Common ETL Config.
 */
public class OldETLConfig extends Config {
  private final OldETLStage source;
  private final List<OldETLStage> sinks;
  private final List<OldETLStage> transforms;
  private final Resources resources;

  public OldETLConfig(OldETLStage source, List<OldETLStage> sinks, List<OldETLStage> transforms, Resources resources) {
    this.source = source;
    this.sinks = sinks;
    this.transforms = transforms;
    this.resources = resources;
  }

  // creates 3.3.x config from the old config.
  public ETLConfig getNewConfig() {
    int pluginNum = 1;
    ETLStage sourceStage = new ETLStage(source.getName() +  "." + pluginNum,
                                        new Plugin(source.getName(), source.getProperties()),
                                        source.getErrorDatasetName());
    List<ETLStage> transformStages = new ArrayList<>();
    if (transforms != null) {
      for (OldETLStage transform : transforms) {
        pluginNum++;
        transformStages.add(new ETLStage(
          transform.getName() + "." + pluginNum,
          new Plugin(transform.getName(), transform.getProperties()),
          transform.getErrorDatasetName()));
      }
    }
    List<ETLStage> sinkStages = new ArrayList<>();
    for (OldETLStage sink : sinks) {
      pluginNum++;
      sinkStages.add(new ETLStage(
        sink.getName() + "." + pluginNum,
        new Plugin(sink.getName(), sink.getProperties()),
        sink.getErrorDatasetName()));
    }

    List<Connection> connections = new ArrayList<>();
    String prevStage = sourceStage.getName();
    for (ETLStage transformStage : transformStages) {
      connections.add(new Connection(prevStage, transformStage.getName()));
      prevStage = transformStage.getName();
    }
    for (ETLStage sinkStage : sinkStages) {
      connections.add(new Connection(prevStage, sinkStage.getName()));
    }

    return new ETLConfig(sourceStage, sinkStages, transformStages, connections, resources);
  }
}
