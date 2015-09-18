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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.Resources;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Common ETL Config.
 */
public class ETLConfig extends Config {
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

  public ETLConfig(ETLStage source, ETLStage sink, List<ETLStage> transforms, Resources resources) {
    this.source = source;
    this.sinks = new ArrayList<>();
    this.sinks.add(sink);
    this.transforms = transforms;
    this.resources = resources;
  }

  public ETLStage getSource() {
    return source;
  }

  public List<ETLStage> getSinks() {
    return sinks;
  }

  public List<ETLStage> getTransforms() {
    return transforms != null ? transforms : Lists.<ETLStage>newArrayList();
  }

  public Resources getResources() {
    return resources;
  }
}
