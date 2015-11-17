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

package co.cask.cdap.etl.batch.config;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLConfig;
import co.cask.cdap.etl.common.ETLStage;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * ETL Batch Configuration.
 */
public final class ETLBatchConfig extends ETLConfig {
  private final String schedule;
  private final List<ETLStage> actions;

  // TODO consider using builder
  public ETLBatchConfig(String schedule, ETLStage source, List<ETLStage> sinks, List<ETLStage> transforms,
                        List<Connection> connections, @Nullable Resources resources, @Nullable List<ETLStage> actions) {
    super(source, sinks, transforms, connections, resources);
    this.schedule = schedule;
    this.actions = actions;
  }

  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink, List<ETLStage> transforms,
                        List<Connection> connections, @Nullable Resources resources, @Nullable List<ETLStage> actions) {
    super(source, sink, transforms, connections, resources);
    this.schedule = schedule;
    this.actions = actions;
  }

  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink, List<ETLStage> transforms,
                        Resources resources) {
    this(schedule, source, sink, transforms, new ArrayList<Connection>(), resources, null);
  }

  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink,
                        List<ETLStage> transforms, List<ETLStage> actions) {
    this(schedule, source, sink, transforms, new ArrayList<Connection>(), null, actions);
  }

  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink,
                        List<ETLStage> transforms, List<Connection> connections, List<ETLStage> actions) {
    this(schedule, source, sink, transforms, connections, null, actions);
  }

  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink, List<ETLStage> transforms,
                        List<Connection> connections, Resources resources) {
    this(schedule, source, sink, transforms, connections, resources, null);
  }

  @VisibleForTesting
  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this(schedule, source, sink, transforms, new ArrayList<Connection>(), null, null);
  }

  @VisibleForTesting
  public ETLBatchConfig(String schedule, ETLStage source, ETLStage sink) {
    this(schedule, source, sink, null);
  }

  public String getSchedule() {
    return schedule;
  }

  public List<ETLStage> getActions() {
    return actions;
  }
}
