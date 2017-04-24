/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.planner.PipelinePlan;
import co.cask.cdap.etl.planner.PipelinePlanner;
import co.cask.cdap.etl.proto.Engine;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.spark.batch.ETLSpark;
import co.cask.cdap.etl.spec.PipelineSpecGenerator;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;

import java.util.HashMap;
import java.util.Set;

/**
 * ETL Batch Application.
 */
public class ETLBatchApplication extends AbstractApplication<ETLBatchConfig> {
  public static final String SCHEDULE_NAME = "etlWorkflow";
  public static final String DEFAULT_DESCRIPTION = "Extract-Transform-Load (ETL) Batch Application";
  private static final Set<String> SUPPORTED_PLUGIN_TYPES = ImmutableSet.of(
    BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE);

  @Override
  public void configure() {
    ETLBatchConfig config = getConfig().convertOldConfig();
    setDescription(DEFAULT_DESCRIPTION);

    PipelineSpecGenerator<ETLBatchConfig, BatchPipelineSpec> specGenerator = new BatchPipelineSpecGenerator(
      getConfigurer(),
      ImmutableSet.of(BatchSource.PLUGIN_TYPE), ImmutableSet.of(BatchSink.PLUGIN_TYPE),
      TimePartitionedFileSet.class,
      FileSetProperties.builder()
        .setInputFormat(AvroKeyInputFormat.class)
        .setOutputFormat(AvroKeyOutputFormat.class)
        .setEnableExploreOnCreate(true)
        .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
        .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
        .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
        .setTableProperty("avro.schema.literal", Constants.ERROR_SCHEMA.toString())
        .build(),
      config.getEngine());

    BatchPipelineSpec spec = specGenerator.generateSpec(config);

    int sourceCount = 0;
    for (StageSpec stageSpec : spec.getStages()) {
      if (BatchSource.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
        sourceCount++;
      }
    }
    if (sourceCount != 1) {
      throw new IllegalArgumentException("Invalid pipeline. There must only be one source.");
    }

    PipelinePlanner planner = new PipelinePlanner(SUPPORTED_PLUGIN_TYPES, ImmutableSet.<String>of(),
                                                  ImmutableSet.<String>of(), ImmutableSet.<String>of());
    PipelinePlan plan = planner.plan(spec);

    if (plan.getPhases().size() != 1) {
      // should never happen if there is only one source
      throw new IllegalArgumentException("There was an error planning the pipeline. There should only be one phase.");
    }

    PipelinePhase pipeline = plan.getPhases().values().iterator().next();

    switch (config.getEngine()) {
      case MAPREDUCE:
        BatchPhaseSpec batchPhaseSpec = new BatchPhaseSpec(ETLMapReduce.NAME, pipeline,
                                                           config.getResources(),
                                                           config.getDriverResources(),
                                                           config.getClientResources(),
                                                           config.isStageLoggingEnabled(),
                                                           config.isProcessTimingEnabled(),
                                                           new HashMap<String, String>(),
                                                           config.getNumOfRecordsPreview(),
                                                           config.getProperties());
        addMapReduce(new ETLMapReduce(batchPhaseSpec));
        break;
      case SPARK:
        batchPhaseSpec = new BatchPhaseSpec(ETLSpark.class.getSimpleName(), pipeline,
                                            config.getResources(),
                                            config.getDriverResources(),
                                            config.getClientResources(),
                                            config.isStageLoggingEnabled(),
                                            config.isProcessTimingEnabled(),
                                            new HashMap<String, String>(), config.getNumOfRecordsPreview(),
                                            config.getProperties());
        addSpark(new ETLSpark(batchPhaseSpec));
        break;
      default:
        throw new IllegalArgumentException(
          String.format("Invalid execution engine '%s'. Must be one of %s.",
                        config.getEngine(), Joiner.on(',').join(Engine.values())));
    }

    addWorkflow(new ETLWorkflow(spec, config.getEngine()));
    scheduleWorkflow(Schedules.builder(SCHEDULE_NAME)
                       .setDescription("ETL Batch schedule")
                       .createTimeSchedule(config.getSchedule()),
                     ETLWorkflow.NAME);
  }
}
