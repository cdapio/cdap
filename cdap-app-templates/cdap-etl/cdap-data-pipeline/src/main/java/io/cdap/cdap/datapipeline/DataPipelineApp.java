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

package io.cdap.cdap.datapipeline;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ApplicationUpgradeContext;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.ScheduleBuilder;
import io.cdap.cdap.datapipeline.service.StudioService;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.condition.Condition;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;

import io.cdap.cdap.etl.proto.v2.ETLStage;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL Data Pipeline Application.
 */
public class DataPipelineApp extends AbstractApplication<ETLBatchConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(DataPipelineApp.class);

  public static final String SCHEDULE_NAME = "dataPipelineSchedule";
  public static final String DEFAULT_DESCRIPTION = "Data Pipeline Application";
  // Jay Pandya: Check validity of plugin type based on supported list here.
  private static final Set<String> supportedPluginTypes = ImmutableSet.of(
    BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE, BatchJoiner.PLUGIN_TYPE,
    Constants.Connector.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE, SparkCompute.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE,
    Action.PLUGIN_TYPE, ErrorTransform.PLUGIN_TYPE, Constants.SPARK_PROGRAM_PLUGIN_TYPE, SplitterTransform.PLUGIN_TYPE,
    Condition.PLUGIN_TYPE, AlertPublisher.PLUGIN_TYPE);
  private static final Gson GSON = new Gson();

  @Override
  public void configure() {
    ETLBatchConfig config = getConfig();

    // if this is for the system services and not an actual pipeline
    if (config.isService()) {
      addService(new StudioService());
      setDescription("Data Pipeline System Services.");
      return;
    }

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

  @Override
  public String updateAppConfig(String configStr, ApplicationUpgradeContext upgradeContext) {
    LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 1");
    ETLBatchConfig batchConfig = GSON.fromJson(configStr, ETLBatchConfig.class);
    LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 2");
    // Jay Pandya: Verify is nothing is missing in deep copy.
    ETLBatchConfig.Builder builder = ETLBatchConfig.builder()
        .setTimeSchedule(batchConfig.getSchedule())
        .addConnections(batchConfig.getConnections())
        .setResources(batchConfig.getResources())
        .setDriverResources(batchConfig.getDriverResources())
        .setEngine(batchConfig.getEngine());
    LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 3");
    // upgrade any of the plugin artifact versions if needed
    for (ETLStage postAction : batchConfig.getPostActions()) {
      LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 4");
      builder.addPostAction(postAction.upgradeStage(upgradeContext));
      LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 5");
    }
    for (ETLStage stage : batchConfig.getStages()) {
      LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 6");
      builder.addStage(stage.upgradeStage(upgradeContext));
      LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 7");
    }
    LOG.info("Jay Pandya reached in updateAppConfig in Data Pipeline app 8");
    return GSON.toJson(builder.build());
  }
}
