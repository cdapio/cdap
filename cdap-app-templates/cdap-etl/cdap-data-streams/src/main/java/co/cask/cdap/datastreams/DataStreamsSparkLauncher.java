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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.SparkConf;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * CDAP Spark client that configures and launches the actual Spark program.
 */
public class DataStreamsSparkLauncher extends AbstractSpark {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamsSparkLauncher.class);
  static final String IS_UNIT_TEST = "hydrator.is.unit.test";
  static final String NUM_SOURCES = "hydrator.num.sources";
  static final String EXTRA_OPTS = "hydrator.extra.opts";
  static final String CHECKPOINT_DIR = "hydrator.checkpoint.dir";
  static final String CHECKPOINTS_DISABLED = "hydrator.checkpoints.disabled";
  public static final String NAME = "DataStreamsSparkStreaming";
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final DataStreamsPipelineSpec pipelineSpec;
  private final DataStreamsConfig config;

  public DataStreamsSparkLauncher(DataStreamsPipelineSpec pipelineSpec, DataStreamsConfig config) {
    this.pipelineSpec = pipelineSpec;
    this.config = config;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setMainClass(SparkStreamingPipelineDriver.class);

    setExecutorResources(pipelineSpec.getResources());
    setDriverResources(pipelineSpec.getDriverResources());

    int numSources = 0;
    for (StageSpec stageSpec : pipelineSpec.getStages()) {
      if (StreamingSource.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
        numSources++;
      }
    }

    // add source, sink, transform ids to the properties. These are needed at runtime to instantiate the plugins
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.PIPELINEID, GSON.toJson(pipelineSpec));
    properties.put(IS_UNIT_TEST, String.valueOf(config.isUnitTest()));
    properties.put(NUM_SOURCES, String.valueOf(numSources));
    properties.put(EXTRA_OPTS, pipelineSpec.getExtraJavaOpts());
    properties.put(CHECKPOINT_DIR,
                   config.getCheckpointDir() == null ? UUID.randomUUID().toString() : config.getCheckpointDir());
    properties.put(CHECKPOINTS_DISABLED, String.valueOf(config.checkpointsDisabled()));
    setProperties(properties);
  }

  @Override
  public void initialize() throws Exception {
    SparkClientContext context = getContext();
    SparkConf sparkConf = new SparkConf();
    // spark... makes you set this to at least the number of receivers (streaming sources)
    // because it holds one thread per receiver, or one core in distributed mode.
    // so... we have to set this hacky master variable based on the isUnitTest setting in the config
    Map<String, String> programProperties = context.getSpecification().getProperties();
    String extraOpts = programProperties.get(EXTRA_OPTS);
    if (extraOpts != null && !extraOpts.isEmpty()) {
      sparkConf.set("spark.driver.extraJavaOptions", extraOpts);
      sparkConf.set("spark.executor.extraJavaOptions", extraOpts);
    }
    Integer numSources = Integer.valueOf(programProperties.get(NUM_SOURCES));
    // without this, stopping will hang on machines with few cores.
    sparkConf.set("spark.rpc.netty.dispatcher.numThreads", String.valueOf(numSources + 2));
    Boolean isUnitTest = Boolean.valueOf(programProperties.get(IS_UNIT_TEST));
    if (isUnitTest) {
      sparkConf.setMaster(String.format("local[%d]", numSources + 1));
    }
    context.setSparkConf(sparkConf);

    boolean checkpointsDisabled = Boolean.valueOf(programProperties.get(CHECKPOINTS_DISABLED));
    if (!checkpointsDisabled) {
      // Each pipeline has its own checkpoint directory within the checkpoint fileset.
      // Ideally, when a pipeline is deleted, we would be able to delete that checkpoint directory.
      // This is because we don't want another pipeline created with the same name to pick up the old checkpoint.
      // Since CDAP has no way to run application logic on deletion, we instead generate a unique pipeline id
      // and use that as the checkpoint directory as a subdirectory inside the pipeline name directory.
      // On start, we check for any other pipeline ids for that pipeline name, and delete them if they exist.
      FileSet checkpointFileSet = context.getDataset(DataStreamsApp.CHECKPOINT_FILESET);
      String pipelineName = context.getApplicationSpecification().getName();
      String checkpointDir = context.getSpecification().getProperty(CHECKPOINT_DIR);
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
  }

  private boolean ensureDirExists(Location location) throws IOException {
    return location.isDirectory() || location.mkdirs() || location.isDirectory();
  }

}
