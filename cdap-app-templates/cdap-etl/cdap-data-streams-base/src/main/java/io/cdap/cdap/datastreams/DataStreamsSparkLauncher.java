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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.LocationAwareMDCWrapperLogger;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * CDAP Spark client that configures and launches the actual Spark program.
 */
public class DataStreamsSparkLauncher extends AbstractSpark {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamsSparkLauncher.class);
  private static final Logger WRAPPERLOGGER = new LocationAwareMDCWrapperLogger(LOG, Constants.EVENT_TYPE_TAG,
                                                                                Constants.PIPELINE_LIFECYCLE_TAG_VALUE);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  public static final String NAME = "DataStreamsSparkStreaming";

  private final DataStreamsPipelineSpec pipelineSpec;

  DataStreamsSparkLauncher(DataStreamsPipelineSpec pipelineSpec) {
    this.pipelineSpec = pipelineSpec;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setMainClass(SparkStreamingPipelineDriver.class);

    setExecutorResources(pipelineSpec.getResources());
    setDriverResources(pipelineSpec.getDriverResources());
    setClientResources(pipelineSpec.getClientResources());

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(pipelineSpec));
    setProperties(properties);
  }

  @TransactionPolicy(TransactionControl.EXPLICIT)
  @Override
  public void initialize() throws Exception {
    SparkClientContext context = getContext();
    String arguments = Joiner.on(", ").withKeyValueSeparator("=").join(context.getRuntimeArguments());
    WRAPPERLOGGER.info("Pipeline '{}' is started by user '{}' with arguments {}",
                       context.getApplicationSpecification().getName(),
                       UserGroupInformation.getCurrentUser().getShortUserName(),
                       arguments);

    DataStreamsPipelineSpec spec = GSON.fromJson(context.getSpecification().getProperty(Constants.PIPELINEID),
                                                 DataStreamsPipelineSpec.class);

    PipelinePluginContext pluginContext = new SparkPipelinePluginContext(context, context.getMetrics(), true, true);
    int numSources = 0;

    for (StageSpec stageSpec : spec.getStages()) {
      if (StreamingSource.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
        StreamingSource<Object> streamingSource = pluginContext.newPluginInstance(stageSpec.getName());
        numSources = numSources + streamingSource.getRequiredExecutors();
      }
    }

    // Add warning message to discourage users from using multiple streaming sources.
    if (numSources > 1) {
      LOG.warn("Using multiple streaming sources in a pipeline is not recommended.");
      getContext().getMetrics().count(Constants.Metrics.STREAMING_MULTI_SOURCE_PIPELINE_RUNS_COUNT, 1);
    }

    SparkConf sparkConf = new SparkConf();
    // we do not do checkpointing during preview. Skip enabling write-ahead logs in that case to avoid spark exception
    if (!spec.isPreviewEnabled(context) && spec.getStateSpec()
      .getMode() == DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING) {
      // required spark configs to prevent data loss during real-time pipeline upgrades
      sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");

      String checkpointDir = spec.getStateSpec().getCheckpointDir();
      String checkpointScheme = null;

      if (!Strings.isNullOrEmpty(checkpointDir)) {
        checkpointScheme = new Path(checkpointDir).toUri().getScheme();
      }

      // For non-local file or HDFS, assuming the FS doesn't support flush (e.g. GCS or S3), hence set
      // the following configurations
      if (!"file".equals(checkpointScheme) && !"hdfs".equals(checkpointScheme)) {
        sparkConf.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true");
        sparkConf.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true");
      }
    }
    sparkConf.set("spark.streaming.backpressure.enabled", "true");
    sparkConf.set("spark.spark.streaming.blockInterval", String.valueOf(spec.getBatchIntervalMillis() / 5));
    // NOTE: If you change this value, also update io.netty.maxDirectMemory in
    // KubeMasterEnvironment
    sparkConf.set("spark.network.maxRemoteBlockSizeFetchToMem", String.valueOf(Integer.MAX_VALUE - 512));

    // spark... makes you set this to at least the number of receivers (streaming sources)
    // because it holds one thread per receiver, or one core in distributed mode.
    // so... we have to set this hacky master variable based on the isUnitTest setting in the config
    String extraOpts = spec.getExtraJavaOpts();
    if (extraOpts != null && !extraOpts.isEmpty()) {
      sparkConf.set("spark.driver.extraJavaOptions", extraOpts);
      sparkConf.set("spark.executor.extraJavaOptions", extraOpts);
    }
    // without this, stopping will hang on machines with few cores.
    sparkConf.set("spark.rpc.netty.dispatcher.numThreads", String.valueOf(numSources + 2));

    sparkConf.setMaster(String.format("local[%d]", numSources + 2));
    sparkConf.set("spark.executor.instances", String.valueOf(numSources + 2));

    if (spec.isUnitTest()) {
      sparkConf.setMaster(String.format("local[%d]", numSources + 1));
    }

    // override defaults with any user provided engine configs
    int minExecutors = numSources + 1;
    for (Map.Entry<String, String> property : spec.getProperties().entrySet()) {
      if ("spark.executor.instances".equals(property.getKey())) {
        // don't let the user set this to something that doesn't make sense
        try {
          int numExecutors = Integer.parseInt(property.getValue());
          if (numExecutors < minExecutors) {
            LOG.warn("Number of executors {} is less than the minimum number required to run the pipeline. "
                       + "Automatically increasing it to {}", numExecutors, minExecutors);
            numExecutors = minExecutors;
          }
          sparkConf.set(property.getKey(), String.valueOf(numExecutors));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
            "Number of spark executors was set to invalid value " + property.getValue(), e);
        }
      } else {
        sparkConf.set(property.getKey(), property.getValue());
      }
      if ("spark.streaming.receiver.writeAheadLog.enable".equals(property.getKey())) {
        boolean isWriteAheadLogEnabled = Boolean.parseBoolean(property.getValue());
        if (spec.getStateSpec().getMode() != DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING && isWriteAheadLogEnabled) {
          throw new IllegalArgumentException(
            "Checkpointing should be enabled when write-ahead logs are enabled for the pipeline.");
        }
        sparkConf.set(property.getKey(), property.getValue());
      }
    }
    context.setSparkConf(sparkConf);
    emitMetrics(spec);
    WRAPPERLOGGER.info("Pipeline '{}' running", context.getApplicationSpecification().getName());
  }

  @TransactionPolicy(TransactionControl.EXPLICIT)
  @Override
  public void destroy() {
    super.destroy();
    ProgramStatus status = getContext().getState().getStatus();
    WRAPPERLOGGER.info("Pipeline '{}' {}", getContext().getApplicationSpecification().getName(),
                       status == ProgramStatus.COMPLETED ? "succeeded" : status.name().toLowerCase());

  }

  private void emitMetrics(DataStreamsPipelineSpec spec) {
    if (spec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.NONE) {
      getContext().getMetrics().count(
        Constants.Metrics.AtleastOnceProcessing.STREAMING_ATLEASTONCE_DISABLED_COUNT, 1);
    } else if (spec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING) {
      getContext().getMetrics().count(
        Constants.Metrics.AtleastOnceProcessing.STREAMING_ATLEASTONCE_CHECKPOINTING_COUNT, 1);
    } else {
      getContext().getMetrics().count(
        Constants.Metrics.AtleastOnceProcessing.STREAMING_ATLEASTONCE_STORE_COUNT, 1);
    }
  }
}
