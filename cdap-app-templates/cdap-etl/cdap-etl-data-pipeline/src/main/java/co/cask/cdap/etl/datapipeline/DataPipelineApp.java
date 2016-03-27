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

package co.cask.cdap.etl.datapipeline;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.planner.PipelinePlan;
import co.cask.cdap.etl.planner.PipelinePlanner;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.PipelineSpecGenerator;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;

import java.util.Set;

/**
 * ETL Data Pipeline Application.
 */
public class DataPipelineApp extends AbstractApplication<ETLBatchConfig> {
  public static final String SCHEDULE_NAME = "dataPipelineSchedule";
  public static final String DEFAULT_DESCRIPTION = "Data Pipeline Application";
  private static final Set<String> supportedPluginTypes = ImmutableSet.of(
    BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE,
    Constants.CONNECTOR_TYPE, BatchAggregator.PLUGIN_TYPE, SparkCompute.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE);

  @Override
  public void configure() {
    ETLBatchConfig config = getConfig();
    setDescription(DEFAULT_DESCRIPTION);

    PipelineSpecGenerator specGenerator =
      new PipelineSpecGenerator(getConfigurer(), ImmutableSet.of(BatchSource.PLUGIN_TYPE),
                                ImmutableSet.of(BatchSink.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE),
                                TimePartitionedFileSet.class,
                                FileSetProperties.builder()
                                  .setInputFormat(AvroKeyInputFormat.class)
                                  .setOutputFormat(AvroKeyOutputFormat.class)
                                  .setEnableExploreOnCreate(true)
                                  .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
                                  .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
                                  .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
                                  .setTableProperty("avro.schema.literal", Constants.ERROR_SCHEMA.toString())
                                  .build());
    PipelineSpec spec = specGenerator.generateSpec(config);

    PipelinePlanner planner = new PipelinePlanner(supportedPluginTypes,
                                                  ImmutableSet.of(BatchAggregator.PLUGIN_TYPE),
                                                  ImmutableSet.of(SparkCompute.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE));
    PipelinePlan plan = planner.plan(spec);

    addWorkflow(new SmartWorkflow(spec, plan, getConfigurer(), config.getEngine()));
    scheduleWorkflow(Schedules.builder(SCHEDULE_NAME)
                       .setDescription("Data pipeline schedule")
                       .createTimeSchedule(config.getSchedule()),
                     SmartWorkflow.NAME);
  }
}
