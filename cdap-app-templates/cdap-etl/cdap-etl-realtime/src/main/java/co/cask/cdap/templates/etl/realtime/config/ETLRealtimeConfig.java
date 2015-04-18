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

package co.cask.cdap.templates.etl.realtime.config;

import co.cask.cdap.api.Resources;
import co.cask.cdap.templates.etl.api.config.ETLStage;

import java.util.List;

/**
 * ETL Realtime Adapter Configuration.
 */
public class ETLRealtimeConfig {
  private final int instances;
  private final ETLStage source;
  private final ETLStage sink;
  private final List<ETLStage> transforms;
  private final Resources resources;

  public ETLRealtimeConfig(int instances, ETLStage source, ETLStage sink, List<ETLStage> transforms,
                           Resources resources) {
    this.instances = instances;
    this.source = source;
    this.sink = sink;
    this.transforms = transforms;
    this.resources = resources;
  }

  public ETLRealtimeConfig(int instances, ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this(instances, source, sink, transforms, null);
  }

  public ETLRealtimeConfig(ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this(1, source, sink, transforms);
  }

  public int getInstances() {
    return instances;
  }

  public ETLStage getSource() {
    return source;
  }

  public ETLStage getSink() {
    return sink;
  }

  public List<ETLStage> getTransforms() {
    return transforms;
  }

  public Resources getResources() {
    return resources;
  }
}
