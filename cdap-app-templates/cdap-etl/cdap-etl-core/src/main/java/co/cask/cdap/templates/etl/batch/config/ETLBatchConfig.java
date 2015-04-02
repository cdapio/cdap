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

package co.cask.cdap.templates.etl.batch.config;

import co.cask.cdap.templates.etl.common.config.ETLStage;

import java.util.List;

/**
 * ETL Batch Adapter Configuration.
 */
public class ETLBatchConfig {
  private final String schedule;
  private final ETLStage source;
  private final ETLStage sink;
  private final List<ETLStage> transforms;

  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this.schedule = schedule;
    this.source = source;
    this.sink = sink;
    this.transforms = transforms;
  }

  public String getSchedule() {
    return schedule;
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
}
