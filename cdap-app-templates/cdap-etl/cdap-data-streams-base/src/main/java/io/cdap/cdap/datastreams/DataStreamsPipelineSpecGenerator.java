/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.datastreams;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.macro.TimeParser;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.spec.PipelineSpecGenerator;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.hadoop.fs.Path;

import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Generates specs for data stream pipelines.
 */
public class DataStreamsPipelineSpecGenerator
  extends PipelineSpecGenerator<DataStreamsConfig, DataStreamsPipelineSpec> {
  private static final Gson GSON = new GsonBuilder()
                                     .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
                                     .create();
  private final RuntimeConfigurer runtimeConfigurer;

  <T extends PluginConfigurer & DatasetConfigurer> DataStreamsPipelineSpecGenerator(
    String namespace, T configurer, @Nullable RuntimeConfigurer runtimeConfigurer, Set<String> sourcePluginTypes,
    Set<String> sinkPluginTypes, FeatureFlagsProvider featureFlagsProvider) {
    super(namespace, configurer, runtimeConfigurer, sourcePluginTypes, sinkPluginTypes,
          Engine.SPARK, featureFlagsProvider);
    this.runtimeConfigurer = runtimeConfigurer;
  }

  @Override
  public DataStreamsPipelineSpec generateSpec(DataStreamsConfig config) throws ValidationException {
    long batchIntervalMillis;
    try {
      batchIntervalMillis = TimeParser.parseDuration(config.getBatchInterval());
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse batchInterval '%s'", config.getBatchInterval()));
    }

    String pipelineId = UUID.randomUUID().toString();
    if (runtimeConfigurer != null && runtimeConfigurer.getDeployedApplicationSpec() != null) {
      SparkSpecification sparkSpec =
        runtimeConfigurer.getDeployedApplicationSpec().getSpark().get(DataStreamsSparkLauncher.NAME);
      DataStreamsPipelineSpec spec = GSON.fromJson(sparkSpec.getProperty(Constants.PIPELINEID),
                                                   DataStreamsPipelineSpec.class);
      pipelineId = spec.getPipelineId();
    }

    DataStreamsPipelineSpec.Builder specBuilder = DataStreamsPipelineSpec.builder(batchIntervalMillis, pipelineId)
      .setExtraJavaOpts(config.getExtraJavaOpts())
      .setStopGracefully(config.getStopGracefully())
      .setIsUnitTest(config.isUnitTest())
      .setCheckpointsDisabled(config.checkpointsDisabled());
    String checkpointDir = config.getCheckpointDir();
    if (!config.checkpointsDisabled() && checkpointDir != null) {
      try {
        new Path(checkpointDir);
      } catch (Exception e) {
        throw new IllegalArgumentException(
          String.format("Checkpoint directory '%s' is not a valid Path: %s", checkpointDir, e.getMessage()), e);
      }
      specBuilder.setCheckpointDirectory(checkpointDir);
    }
    configureStages(config, specBuilder);
    return specBuilder.build();
  }

  @Override
  protected void validateJoinCondition(String stageName, JoinCondition condition, FailureCollector collector) {
    if (condition.getOp() != JoinCondition.Op.KEY_EQUALITY) {
      collector.addFailure(
        String.format("Join stage '%s' uses a %s condition, which is not supported in streaming pipelines.",
                      stageName, condition.getOp()),
        "Only basic joins on key equality are supported.");
    }
  }
}
