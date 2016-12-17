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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.twill.filesystem.Location;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Driver for running pipelines using Spark Streaming.
 */
public class SparkStreamingPipelineDriver implements JavaSparkMain {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Set<String> SUPPORTED_PLUGIN_TYPES = ImmutableSet.of(
    StreamingSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE,
    BatchJoiner.PLUGIN_TYPE, SparkCompute.PLUGIN_TYPE, Windower.PLUGIN_TYPE);

  @Override
  public void run(final JavaSparkExecutionContext sec) throws Exception {
    final DataStreamsPipelineSpec pipelineSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
                                                               DataStreamsPipelineSpec.class);

    PipelinePhase.Builder phaseBuilder = PipelinePhase.builder(SUPPORTED_PLUGIN_TYPES)
      .addConnections(pipelineSpec.getConnections());
    for (StageSpec stageSpec : pipelineSpec.getStages()) {
      phaseBuilder.addStage(StageInfo.builder(stageSpec.getName(), stageSpec.getPlugin().getType())
                              .addInputs(stageSpec.getInputs())
                              .addOutputs(stageSpec.getOutputs())
                              .addInputSchemas(stageSpec.getInputSchemas())
                              .setOutputSchema(stageSpec.getOutputSchema())
                              .build());
    }
    final PipelinePhase pipelinePhase = phaseBuilder.build();

    boolean checkpointsDisabled = Boolean.parseBoolean(
      sec.getSpecification().getProperty(DataStreamsSparkLauncher.CHECKPOINTS_DISABLED));

    String checkpointDir = null;
    if (!checkpointsDisabled) {
      // Get the location of the checkpoint directory.
      String pipelineName = sec.getApplicationSpecification().getName();
      String pipelineId = sec.getSpecification().getProperty(DataStreamsSparkLauncher.CHECKPOINT_DIR);

      // there isn't any way to instantiate the fileset except in a TxRunnable, so need to use a reference.
      final AtomicReference<Location> checkpointBaseRef = new AtomicReference<>();
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          FileSet checkpointFileSet = context.getDataset(DataStreamsApp.CHECKPOINT_FILESET);
          checkpointBaseRef.set(checkpointFileSet.getBaseLocation());
        }
      });
      Location pipelineCheckpointDir = checkpointBaseRef.get().append(pipelineName).append(pipelineId);
      checkpointDir = pipelineCheckpointDir.toURI().toString();
    }

    JavaStreamingContext jssc = run(pipelineSpec, pipelinePhase, sec, checkpointDir);
    jssc.start();
    boolean stopped = false;
    try {
      // most programs will just keep running forever.
      // however, when CDAP stops the program, we get an interrupted exception.
      // at that point, we need to call stop on jssc, otherwise the program will hang and never stop.
      stopped = jssc.awaitTerminationOrTimeout(Long.MAX_VALUE);
    } finally {
      if (!stopped) {
        jssc.stop(true, pipelineSpec.isStopGracefully());
      }
    }
  }

  private JavaStreamingContext run(final DataStreamsPipelineSpec pipelineSpec,
                                   final PipelinePhase pipelinePhase,
                                   final JavaSparkExecutionContext sec,
                                   @Nullable final String checkpointDir) throws Exception {

    JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
      @Override
      public JavaStreamingContext create() {
        JavaStreamingContext jssc = new JavaStreamingContext(
          new JavaSparkContext(), Durations.milliseconds(pipelineSpec.getBatchIntervalMillis()));
        SparkStreamingPipelineRunner runner = new SparkStreamingPipelineRunner(sec, jssc, false,
                                                                               pipelineSpec.getNumOfRecordsPreview());
        // TODO: figure out how to get partitions to use for aggregators and joiners.
        // Seems like they should be set at configure time instead of runtime? but that requires an API change.
        try {
          runner.runPipeline(pipelinePhase, StreamingSource.PLUGIN_TYPE, sec, new HashMap<String, Integer>());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        if (checkpointDir != null) {
          jssc.checkpoint(checkpointDir);
        }
        return jssc;
      }
    };

    return JavaStreamingContext.getOrCreate(checkpointDir, contextFactory);
  }


}
