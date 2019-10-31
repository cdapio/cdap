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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.schedule.ScheduleBuilder;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkJoiner;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * ETL Data Pipeline Application.
 */
public class DataPipelineApp extends AbstractApplication<ETLBatchConfig> {
  public static final String SCHEDULE_NAME = "dataPipelineSchedule";
  public static final String DEFAULT_DESCRIPTION = "Data Pipeline Application";
  private static final Set<String> supportedPluginTypes = ImmutableSet.of(
    BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE, BatchJoiner.PLUGIN_TYPE,
    Constants.Connector.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE, SparkJoiner.PLUGIN_TYPE, SparkCompute.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE,
    Action.PLUGIN_TYPE, ErrorTransform.PLUGIN_TYPE, Constants.SPARK_PROGRAM_PLUGIN_TYPE, SplitterTransform.PLUGIN_TYPE,
    Condition.PLUGIN_TYPE, AlertPublisher.PLUGIN_TYPE);

  @Override
  public void configure() {
    ETLBatchConfig config = getConfig();
    setDescription(Objects.firstNonNull(config.getDescription(), DEFAULT_DESCRIPTION));

    addWorkflow(new SmartWorkflow(config, supportedPluginTypes, getConfigurer()));

    String timeSchedule = config.getSchedule();
    if (timeSchedule != null) {
      ScheduleBuilder scheduleBuilder = buildSchedule(SCHEDULE_NAME, ProgramType.WORKFLOW, SmartWorkflow.NAME)
        .setDescription("Data pipeline schedule");
      Integer maxConcurrentRuns = config.getMaxConcurrentRuns();
      if (maxConcurrentRuns != null) {
        scheduleBuilder.withConcurrency(maxConcurrentRuns);
      }
      schedule(scheduleBuilder.triggerByTime(timeSchedule));
    }
  }
}
