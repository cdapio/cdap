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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.etl.batch.mapreduce.ETLMapReduce;
import co.cask.cdap.etl.batch.spark.ETLSpark;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRegisterer;
import co.cask.cdap.etl.proto.v1.ETLBatchConfig;
import com.google.common.base.Joiner;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;

import java.util.HashMap;

/**
 * ETL Batch Application.
 */
public class ETLBatchApplication extends AbstractApplication<ETLBatchConfig> {
  public static final String SCHEDULE_NAME = "etlWorkflow";
  public static final String DEFAULT_DESCRIPTION = "Extract-Transform-Load (ETL) Batch Application";

  @Override
  public void configure() {
    ETLBatchConfig config = getConfig();
    setDescription(DEFAULT_DESCRIPTION);

    PipelineRegisterer pipelineRegisterer = new PipelineRegisterer(getConfigurer(), "batch");

    PipelinePhase pipeline =
      pipelineRegisterer.registerPlugins(
        config, TimePartitionedFileSet.class,
        FileSetProperties.builder()
          .setInputFormat(AvroKeyInputFormat.class)
          .setOutputFormat(AvroKeyOutputFormat.class)
          .setEnableExploreOnCreate(true)
          .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
          .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
          .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
          .setTableProperty("avro.schema.literal", Constants.ERROR_SCHEMA.toString())
          .build(), true);

    switch (config.getEngine()) {
      case MAPREDUCE:
        BatchPhaseSpec batchPhaseSpec = new BatchPhaseSpec(ETLMapReduce.NAME, pipeline,
                                                           config.getResources(),
                                                           config.isStageLoggingEnabled(),
                                                           new HashMap<String, String>());
        addMapReduce(new ETLMapReduce(batchPhaseSpec));
        break;
      case SPARK:
        batchPhaseSpec = new BatchPhaseSpec(ETLSpark.class.getSimpleName(), pipeline,
                                            config.getResources(),
                                            config.isStageLoggingEnabled(),
                                            new HashMap<String, String>());
        addSpark(new ETLSpark(batchPhaseSpec));
        break;
      default:
        throw new IllegalArgumentException(
          String.format("Invalid execution engine '%s'. Must be one of %s.",
                        config.getEngine(), Joiner.on(',').join(ETLBatchConfig.Engine.values())));
    }

    addWorkflow(new ETLWorkflow(config));
    scheduleWorkflow(Schedules.builder(SCHEDULE_NAME)
                       .setDescription("ETL Batch schedule")
                       .createTimeSchedule(config.getSchedule()),
                     ETLWorkflow.NAME);
  }
}
