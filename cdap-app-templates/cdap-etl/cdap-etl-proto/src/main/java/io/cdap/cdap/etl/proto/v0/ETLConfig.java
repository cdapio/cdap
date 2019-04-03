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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.UpgradeContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Common ETL Config.
 */
public abstract class ETLConfig extends Config {
  private final ETLStage source;
  private final List<ETLStage> sinks;
  private final List<ETLStage> transforms;
  private final Resources resources;

  public ETLConfig(ETLStage source, List<ETLStage> sinks, List<ETLStage> transforms, Resources resources) {
    this.source = source;
    this.sinks = sinks;
    this.transforms = transforms;
    this.resources = resources;
  }

  public ETLStage getSource() {
    return source;
  }

  public List<ETLStage> getSinks() {
    return Collections.unmodifiableList(sinks == null ? new ArrayList<ETLStage>() : sinks);
  }

  public List<ETLStage> getTransforms() {
    return Collections.unmodifiableList(transforms == null ? new ArrayList<ETLStage>() : transforms);
  }

  public Resources getResources() {
    return resources == null ? new Resources() : resources;
  }

  protected <T extends co.cask.cdap.etl.proto.v1.ETLConfig.Builder> T upgradeBase(T builder,
                                                                                  UpgradeContext upgradeContext,
                                                                                  String sourceType,
                                                                                  String sinkType) {
    builder.setResources(resources);
    String prevStageName = getSource().getName() + ".1";

    builder.setSource(getSource().upgradeStage(prevStageName, sourceType, upgradeContext));

    int stageNum = 2;
    for (ETLStage v0Transform : getTransforms()) {
      String currStageName = v0Transform.getName() + "." + stageNum;
      builder.addTransform(v0Transform.upgradeStage(currStageName, Transform.PLUGIN_TYPE, upgradeContext));
      builder.addConnection(prevStageName, currStageName);
      prevStageName = currStageName;
      stageNum++;
    }

    for (ETLStage v0Sink : getSinks()) {
      String currStageName = v0Sink.getName() + "." + stageNum;
      builder.addSink(v0Sink.upgradeStage(currStageName, sinkType, upgradeContext));
      builder.addConnection(prevStageName, currStageName);
      stageNum++;
    }

    return builder;
  }
}
