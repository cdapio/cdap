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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.streaming.StreamingStateHandler;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultStageConfigurer;
import io.cdap.cdap.etl.common.macro.TimeParser;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.streaming.StreamingRetrySettings;
import io.cdap.cdap.etl.spec.PipelineSpecGenerator;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.hadoop.fs.Path;

import java.util.HashSet;
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
  //Default high value for max retry - 6 hours
  private static final long defaultMaxRetryTimeInMins = 360L;
  //Default retry delay - 1 second
  private static final long defaultBaseRetryDelayInSeconds = 1L;
  //Default max retry delay - 60 second
  private static final long defaultMaxRetryDelayInSeconds = 60L;
  private final RuntimeConfigurer runtimeConfigurer;
  //Set of sources in this pipeline that supports @link{StreamingStateHandler}
  private Set<String> stateHandlingSources;
  private Set<String> sourcePluginTypes;

  <T extends PluginConfigurer & DatasetConfigurer> DataStreamsPipelineSpecGenerator(
    String namespace, T configurer, @Nullable RuntimeConfigurer runtimeConfigurer, Set<String> sourcePluginTypes,
    Set<String> sinkPluginTypes, FeatureFlagsProvider featureFlagsProvider) {
    super(namespace, configurer, runtimeConfigurer, sourcePluginTypes, sinkPluginTypes,
          Engine.SPARK, featureFlagsProvider);
    this.runtimeConfigurer = runtimeConfigurer;
    this.sourcePluginTypes = sourcePluginTypes;
    this.stateHandlingSources = new HashSet<>();
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
      DataStreamsPipelineSpec spec = getDataStreamsPipelineSpec(runtimeConfigurer.getDeployedApplicationSpec());
      pipelineId = spec.getPipelineId();
    }

    DataStreamsPipelineSpec.Builder specBuilder = DataStreamsPipelineSpec.builder(batchIntervalMillis, pipelineId)
      .setExtraJavaOpts(config.getExtraJavaOpts())
      .setStopGracefully(config.getStopGracefully())
      .setIsUnitTest(config.isUnitTest());

    configureStages(config, specBuilder);
    //Configure the at least once processing mode for this pipeline
    configureAtleastOnceMode(config, specBuilder);
    //Configure retries
    configureRetries(specBuilder);
    return specBuilder.build();
  }

  /**
   * Extract and return {@link DataStreamsPipelineSpec} from the {@link ApplicationSpecification}.
   * @param deployedApplicationSpec
   * @return {@link DataStreamsPipelineSpec}
   */
  static DataStreamsPipelineSpec getDataStreamsPipelineSpec(ApplicationSpecification deployedApplicationSpec) {
    SparkSpecification sparkSpec =
      deployedApplicationSpec.getSpark().get(DataStreamsSparkLauncher.NAME);
    return GSON.fromJson(sparkSpec.getProperty(Constants.PIPELINEID), DataStreamsPipelineSpec.class);
  }

  @VisibleForTesting
  void configureRetries(DataStreamsPipelineSpec.Builder specBuilder) {
    long maxRetryTimeInMins = getFromRuntimeArgs(Constants.CDAP_STREAMING_MAX_RETRY_TIME_IN_MINS,
                                                 defaultMaxRetryTimeInMins);
    long baseRetryDelayInSeconds = getFromRuntimeArgs(Constants.CDAP_STREAMING_BASE_RETRY_DELAY_IN_SECONDS,
                                                      defaultBaseRetryDelayInSeconds);
    long maxRetryDelayInSeconds = getFromRuntimeArgs(Constants.CDAP_STREAMING_MAX_RETRY_DELAY_IN_SECONDS,
                                                     defaultMaxRetryDelayInSeconds);
    StreamingRetrySettings streamingRetrySettings = new StreamingRetrySettings(maxRetryTimeInMins,
                                                                               baseRetryDelayInSeconds,
                                                                               maxRetryDelayInSeconds);
    specBuilder.setStreamingRetrySettings(streamingRetrySettings);
  }

  private long getFromRuntimeArgs(String arg, long defaultValue) {
    if (runtimeConfigurer == null) {
      return defaultValue;
    }

    if (runtimeConfigurer.getRuntimeArguments() == null) {
      return defaultValue;
    }

    String value = runtimeConfigurer.getRuntimeArguments().get(arg);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }

    return Long.parseLong(value);
  }

  @VisibleForTesting
  void configureAtleastOnceMode(DataStreamsConfig config, DataStreamsPipelineSpec.Builder specBuilder) {
    //If runtime arg sets atleast once processing to false, disable both
    if (runtimeConfigurer != null && runtimeConfigurer.getRuntimeArguments() != null) {
      boolean atleastOnceProcessingEnabled = Boolean.parseBoolean(
        runtimeConfigurer.getRuntimeArguments().getOrDefault(Constants.CDAP_STREAMING_ATLEASTONCE_ENABLED, "true"));
      if (!atleastOnceProcessingEnabled) {
        DataStreamsStateSpec stateSpec = DataStreamsStateSpec.getBuilder(DataStreamsStateSpec.Mode.NONE).build();
        specBuilder.setStateSpec(stateSpec);
        return;
      }
    }
    //Check if native state tracking is possible
    if (nativeStateTrackingSupported(config, sourcePluginTypes, stateHandlingSources)) {
      // Native state tracking and spark checkpointing is mutually exclusive
      // This is because Spark recreates context from checkpoint data
      DataStreamsStateSpec stateSpec = DataStreamsStateSpec.getBuilder(DataStreamsStateSpec.Mode.STATE_STORE)
        .build();
      specBuilder.setStateSpec(stateSpec);
    } else {
      String checkpointDir = config.getCheckpointDir();
      if (checkpointDir != null) {
        try {
          new Path(checkpointDir);
        } catch (Exception e) {
          throw new IllegalArgumentException(
            String.format("Checkpoint directory '%s' is not a valid Path: %s", checkpointDir, e.getMessage()), e);
        }
      }
      DataStreamsStateSpec.Builder builder = DataStreamsStateSpec.getBuilder(
        DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING).setCheckPointDir(checkpointDir);
      specBuilder.setStateSpec(builder.build());
    }
  }

  private boolean nativeStateTrackingSupported(DataStreamsConfig config, Set<String> sourcePluginTypes,
                                               Set<String> stateHandlingSources) {
    if (!Feature.STREAMING_PIPELINE_NATIVE_STATE_TRACKING.isEnabled(getFeatureFlagsProvider())) {
      return false;
    }

    // Should have a source plugin that supports native state tracking
    if (stateHandlingSources.isEmpty()) {
      return false;
    }

    // Additional validations.
    // Pipelines with multiple sources and Windower plugin does not work with native state handling.
    int sourceCount = 0;
    for (ETLStage stage : config.getStages()) {
      if (sourcePluginTypes.contains(stage.getPlugin().getType())) {
        sourceCount++;
        if (sourceCount > 1) {
          return false;
        }
      }
      if (stage.getPlugin().getType() == Windower.PLUGIN_TYPE) {
        return false;
      }
    }

    return true;
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

  @Override
  protected void configureSourcePlugin(String stageName, Object plugin, DefaultStageConfigurer stageConfigurer,
                                       FailureCollector collector) {
    super.configureSourcePlugin(stageName, plugin, stageConfigurer, collector);
    if (plugin instanceof StreamingStateHandler) {
      stateHandlingSources.add(stageName);
    }
  }
}
