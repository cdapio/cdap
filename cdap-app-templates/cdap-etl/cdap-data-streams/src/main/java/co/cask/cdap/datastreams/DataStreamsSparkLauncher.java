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

package co.cask.cdap.datastreams;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    int numSources = 0;
    for (StageSpec stageSpec : spec.getStages()) {
      if (StreamingSource.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
        numSources++;
      }
    }

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.streaming.backpressure.enabled", "true");
    for (Map.Entry<String, String> property : spec.getProperties().entrySet()) {
      sparkConf.set(property.getKey(), property.getValue());
    }

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
    if (spec.isUnitTest()) {
      sparkConf.setMaster(String.format("local[%d]", numSources + 1));
    }
    context.setSparkConf(sparkConf);

    if (!spec.isCheckpointsDisabled()) {
      // Each pipeline has its own checkpoint directory within the checkpoint fileset.
      // Ideally, when a pipeline is deleted, we would be able to delete that checkpoint directory.
      // This is because we don't want another pipeline created with the same name to pick up the old checkpoint.
      // Since CDAP has no way to run application logic on deletion, we instead generate a unique pipeline id
      // and use that as the checkpoint directory as a subdirectory inside the pipeline name directory.
      // On start, we check for any other pipeline ids for that pipeline name, and delete them if they exist.
      FileSet checkpointFileSet = context.getDataset(DataStreamsApp.CHECKPOINT_FILESET);
      String pipelineName = context.getApplicationSpecification().getName();
      String checkpointDir = spec.getCheckpointDirectory();
      Location pipelineCheckpointBase = checkpointFileSet.getBaseLocation().append(pipelineName);
      Location pipelineCheckpointDir = pipelineCheckpointBase.append(checkpointDir);

      if (!ensureDirExists(pipelineCheckpointBase)) {
        throw new IOException(
          String.format("Unable to create checkpoint base directory '%s' for the pipeline.", pipelineCheckpointBase));
      }

      try {
        for (Location child : pipelineCheckpointBase.list()) {
          if (!child.equals(pipelineCheckpointDir) && !child.delete(true)) {
            LOG.warn("Unable to delete checkpoint directory {} from an old pipeline.", child);
          }
        }
      } catch (Exception e) {
        LOG.warn("Unable to clean up old checkpoint directories from old pipelines.", e);
      }

      if (!ensureDirExists(pipelineCheckpointDir)) {
        throw new IOException(
          String.format("Unable to create checkpoint directory '%s' for the pipeline.", pipelineCheckpointDir));
      }
    }
    WRAPPERLOGGER.info("Pipeline '{}' running", context.getApplicationSpecification().getName());
  }

  @Override
  public void destroy() {
    super.destroy();
    ProgramStatus status = getContext().getState().getStatus();
    WRAPPERLOGGER.info("Pipeline '{}' {}", getContext().getApplicationSpecification().getName(),
                       status == ProgramStatus.COMPLETED ? "succeeded" : status.name().toLowerCase());

  }

  private boolean ensureDirExists(Location location) throws IOException {
    return location.isDirectory() || location.mkdirs() || location.isDirectory();
  }

}
