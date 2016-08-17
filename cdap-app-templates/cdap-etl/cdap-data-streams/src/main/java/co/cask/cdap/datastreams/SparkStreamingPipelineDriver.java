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
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPipelineDriver;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.streaming.DStreamCollection;
import co.cask.cdap.etl.spark.streaming.DefaultStreamingContext;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Set;

/**
 * Driver for running pipelines using Spark Streaming.
 */
public class SparkStreamingPipelineDriver extends SparkPipelineDriver implements JavaSparkMain {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Set<String> SUPPORTED_PLUGIN_TYPES = ImmutableSet.of(
    StreamingSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE,
    BatchJoiner.PLUGIN_TYPE, SparkCompute.PLUGIN_TYPE, Windower.PLUGIN_TYPE);
  private JavaSparkExecutionContext sec;
  private JavaSparkContext sparkContext;
  private JavaStreamingContext streamingContext;

  @Override
  protected SparkCollection<Object> getSource(String stageName,
                                              PluginFunctionContext pluginFunctionContext) throws Exception {
    StreamingSource<Object> source = sec.getPluginContext().newPluginInstance(stageName);
    StreamingContext context = new DefaultStreamingContext(stageName, sec, streamingContext);
    return new DStreamCollection<>(sec, sparkContext, source.getStream(context));
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    this.sec = sec;

    DataStreamsPipelineSpec pipelineSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
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
    PipelinePhase pipelinePhase = phaseBuilder.build();

    sparkContext = new JavaSparkContext();
    streamingContext = new JavaStreamingContext(sparkContext,
                                                Durations.milliseconds(pipelineSpec.getBatchIntervalMillis()));
    // TODO: figure out how to get partitions to use for aggregators and joiners.
    // Seems like they should be set at configure time instead of runtime? but that requires an API change.
    runPipeline(pipelinePhase, StreamingSource.PLUGIN_TYPE, sec, new HashMap<String, Integer>());

    streamingContext.start();
    boolean stopped = false;
    try {
      // most programs will just keep running forever.
      // however, when CDAP stops the program, we get an interrupted exception.
      // at that point, we need to call stop on jssc, otherwise the program will hang and never stop.
      stopped = streamingContext.awaitTerminationOrTimeout(Long.MAX_VALUE);
    } finally {
      if (!stopped) {
        streamingContext.stop(true, true);
      }
    }
  }
}
