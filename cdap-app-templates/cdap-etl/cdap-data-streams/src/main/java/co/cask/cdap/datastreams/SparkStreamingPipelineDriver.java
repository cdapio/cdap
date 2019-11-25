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

import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkJoiner;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.spark.StreamingCompat;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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
    StreamingSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE,
    BatchAggregator.PLUGIN_TYPE, BatchJoiner.PLUGIN_TYPE, SparkCompute.PLUGIN_TYPE, SparkJoiner.PLUGIN_TYPE,
    Windower.PLUGIN_TYPE,ErrorTransform.PLUGIN_TYPE, SplitterTransform.PLUGIN_TYPE, AlertPublisher.PLUGIN_TYPE);

  @Override
  public void run(final JavaSparkExecutionContext sec) throws Exception {

    final DataStreamsPipelineSpec pipelineSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
                                                               DataStreamsPipelineSpec.class);

    final PipelinePhase pipelinePhase = PipelinePhase.builder(SUPPORTED_PLUGIN_TYPES)
      .addConnections(pipelineSpec.getConnections())
      .addStages(pipelineSpec.getStages())
      .build();

    boolean checkpointsDisabled = pipelineSpec.isCheckpointsDisabled();

    String checkpointDir = null;
    if (!checkpointsDisabled) {
      // Get the location of the checkpoint directory.
      String pipelineName = sec.getApplicationSpecification().getName();
      String relativeCheckpointDir = pipelineSpec.getCheckpointDirectory();

      // there isn't any way to instantiate the fileset except in a TxRunnable, so need to use a reference.
      final AtomicReference<Location> checkpointBaseRef = new AtomicReference<>();
      Transactionals.execute(sec, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          FileSet checkpointFileSet = context.getDataset(DataStreamsApp.CHECKPOINT_FILESET);
          checkpointBaseRef.set(checkpointFileSet.getBaseLocation());
        }
      }, Exception.class);

      Location pipelineCheckpointDir = checkpointBaseRef.get().append(pipelineName).append(relativeCheckpointDir);
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

    Function0<JavaStreamingContext> contextFunction = new Function0<JavaStreamingContext>() {
      @Override
      public JavaStreamingContext call() throws Exception {
        JavaStreamingContext jssc = new JavaStreamingContext(
          new JavaSparkContext(), Durations.milliseconds(pipelineSpec.getBatchIntervalMillis()));
        SparkStreamingPipelineRunner runner = new SparkStreamingPipelineRunner(sec, jssc, pipelineSpec, 
                                                                               pipelineSpec.isCheckpointsDisabled());
        PipelinePluginContext pluginContext = new PipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                        pipelineSpec.isStageLoggingEnabled(),
                                                                        pipelineSpec.isProcessTimingEnabled());
        // TODO: figure out how to get partitions to use for aggregators and joiners.
        // Seems like they should be set at configure time instead of runtime? but that requires an API change.
        try {
          runner.runPipeline(pipelinePhase, StreamingSource.PLUGIN_TYPE,
                             sec, new HashMap<String, Integer>(), pluginContext,
                             new HashMap<String, StageStatisticsCollector>(), jssc.sparkContext());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        if (checkpointDir != null) {
          jssc.checkpoint(checkpointDir);
        }
        return jssc;
      }
    };
    return checkpointDir == null ? contextFunction.call() : StreamingCompat.getOrCreate(checkpointDir, contextFunction);
  }
}
