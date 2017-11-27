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

package co.cask.cdap.etl.proto.v2;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.Connection;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * ETL Batch Configuration.
 */
public final class ETLBatchConfig extends ETLConfig {
  private final Engine engine;
  private final String schedule;
  private final Integer maxConcurrentRuns;
  private final List<ETLStage> postActions;
  // for backwards compatibility
  private final List<ETLStage> actions;

  private ETLBatchConfig(Set<ETLStage> stages,
                         Set<Connection> connections,
                         List<ETLStage> postActions,
                         Resources resources,
                         boolean stageLoggingEnabled,
                         boolean processTimingEnabled,
                         Engine engine,
                         String schedule,
                         Resources driverResources,
                         Resources clientResources,
                         int numOfRecordsPreview,
                         @Nullable Integer maxConcurrentRuns,
                         Map<String, String> engineProperties) {
    super(stages, connections, resources, driverResources, clientResources, stageLoggingEnabled, processTimingEnabled,
          numOfRecordsPreview, engineProperties);
    this.postActions = ImmutableList.copyOf(postActions);
    this.engine = engine;
    this.schedule = schedule;
    this.maxConcurrentRuns = maxConcurrentRuns;
    // field only exists for backwards compatibility -- used by convertOldConfig()
    this.actions = null;
  }

  /**
   * If this has the old v1 fields (source, sinks, transforms), convert them all to stages and create a proper
   * v2 config. If this is already a v2 config, just returns itself.
   * This method is only here to support backwards compatibility.
   *
   * @return A v2 config.
   */
  public ETLBatchConfig convertOldConfig() {
    if (!getStages().isEmpty()) {
      return this;
    }

    ETLBatchConfig.Builder builder = builder(schedule)
      .setEngine(engine)
      .setDriverResources(getDriverResources())
      .addPostActions(actions == null ? new HashSet<ETLStage>() : actions);
    return convertStages(builder, BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE).build();
  }

  public List<ETLStage> getPostActions() {
    return Collections.unmodifiableList(postActions == null ? new ArrayList<ETLStage>() : postActions);
  }

  public Engine getEngine() {
    return engine == null ? Engine.MAPREDUCE : engine;
  }

  public String getSchedule() {
    return schedule;
  }

  @Nullable
  public Integer getMaxConcurrentRuns() {
    return maxConcurrentRuns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ETLBatchConfig that = (ETLBatchConfig) o;

    return Objects.equals(engine, that.engine) &&
      Objects.equals(schedule, that.schedule) &&
      Objects.equals(postActions, that.postActions) &&
      Objects.equals(actions, that.actions) &&
      Objects.equals(maxConcurrentRuns, that.maxConcurrentRuns);

  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), engine, schedule, postActions, actions, maxConcurrentRuns);
  }

  @Override
  public String toString() {
    return "ETLBatchConfig{" +
      "engine=" + engine +
      ", schedule='" + schedule + '\'' +
      ", maxConcurrentRuns=" + maxConcurrentRuns +
      ", postActions=" + postActions +
      ", actions=" + actions +
      "} " + super.toString();
  }

  public static Builder builder(String schedule) {
    return new Builder(schedule);
  }

  /**
   * Builder for creating configs.
   */
  public static class Builder extends ETLConfig.Builder<Builder> {
    private final String schedule;
    private Engine engine;
    private List<ETLStage> endingActions;
    private Integer maxConcurrentRuns;

    public Builder(String schedule) {
      super();
      this.schedule = schedule;
      this.engine = Engine.MAPREDUCE;
      this.endingActions = new ArrayList<>();
    }

    public Builder setEngine(Engine engine) {
      this.engine = engine;
      return this;
    }

    public Builder setDriverResources(Resources driverResources) {
      this.driverResources = driverResources;
      return this;
    }

    public Builder addPostAction(ETLStage action) {
      this.endingActions.add(action);
      return this;
    }

    public Builder addPostActions(Collection<ETLStage> actions) {
      this.endingActions.addAll(actions);
      return this;
    }

    public Builder setMaxConcurrentRuns(int maxConcurrentRuns) {
      this.maxConcurrentRuns = maxConcurrentRuns;
      return this;
    }

    public ETLBatchConfig build() {
      return new ETLBatchConfig(stages, connections, endingActions, resources, stageLoggingEnabled,
                                processTimingEnabled, engine, schedule, driverResources, clientResources,
                                numOfRecordsPreview, maxConcurrentRuns, properties);
    }
  }
}
