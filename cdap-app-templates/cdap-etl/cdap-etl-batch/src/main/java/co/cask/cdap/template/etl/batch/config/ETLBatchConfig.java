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

package co.cask.cdap.template.etl.batch.config;

import co.cask.cdap.api.Resources;
import co.cask.cdap.template.etl.common.ETLConfig;
import co.cask.cdap.template.etl.common.ETLStage;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

/**
 * ETL Batch Adapter Configuration.
 */
public final class ETLBatchConfig extends ETLConfig {
  private final String schedule;

  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink, List<ETLStage> transforms,
                        Resources resources) {
    super(source, sink, transforms, resources);
    this.schedule = schedule;
  }

  @VisibleForTesting
  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this(schedule, source, sink, transforms, null);
  }

  @VisibleForTesting
  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink) {
    this(schedule, source, sink, null);
  }

  public String getSchedule() {
    return schedule;
  }
}
