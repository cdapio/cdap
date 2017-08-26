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

import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.SparkPipelineRunner;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import co.cask.cdap.etl.spark.streaming.DStreamCollection;
import co.cask.cdap.etl.spark.streaming.DefaultStreamingContext;
import co.cask.cdap.etl.spark.streaming.DynamicDriverContext;
import co.cask.cdap.etl.spark.streaming.PairDStreamCollection;
import co.cask.cdap.etl.spark.streaming.function.CountingTransformFunction;
import co.cask.cdap.etl.spark.streaming.function.DynamicJoinMerge;
import co.cask.cdap.etl.spark.streaming.function.DynamicJoinOn;
import co.cask.cdap.etl.spark.streaming.function.WrapOutputTransformFunction;
import co.cask.cdap.etl.spark.streaming.function.preview.LimitingFunction;
import co.cask.cdap.etl.spec.StageSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.List;

/**
 * Driver for running pipelines using Spark Streaming.
 */
public class SparkStreamingPipelineRunner extends SparkPipelineRunner {

  private final JavaSparkExecutionContext sec;
  private final JavaStreamingContext streamingContext;
  private final DataStreamsPipelineSpec spec;
  private final boolean checkpointsDisabled;

  public SparkStreamingPipelineRunner(JavaSparkExecutionContext sec, JavaStreamingContext streamingContext,
                                      DataStreamsPipelineSpec spec, boolean checkpointsDisabled) {
    this.sec = sec;
    this.streamingContext = streamingContext;
    this.checkpointsDisabled = checkpointsDisabled;
    this.spec = spec;
  }

  @Override
  protected SparkCollection<RecordInfo<Object>> getSource(StageSpec stageSpec,
                                                          StageStatisticsCollector collector) throws Exception {
    StreamingSource<Object> source;
    if (checkpointsDisabled) {
      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
      source = pluginFunctionContext.createPlugin();
    } else {
      // check for macros in any StreamingSource. If checkpoints are enabled,
      // SparkStreaming will serialize all InputDStreams created in the checkpoint, which means
      // the InputDStream is deserialized directly from the checkpoint instead of instantiated through CDAP.
      // This means there isn't any way for us to perform macro evaluation on sources when they are loaded from
      // checkpoints. We can work around this in all other pipeline stages by dynamically instantiating the
      // plugin in all DStream functions, but can't for InputDStreams because the InputDStream constructor
      // adds itself to the context dag. Yay for constructors with global side effects.
      // TODO: (HYDRATOR-1030) figure out how to do this at configure time instead of run time
      MacroEvaluator macroEvaluator = new ErrorMacroEvaluator(
        "Due to spark limitations, macro evaluation is not allowed in streaming sources when checkpointing " +
          "is enabled.");
      PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                   spec.isStageLoggingEnabled(),
                                                                   spec.isProcessTimingEnabled());
      source = pluginContext.newPluginInstance(stageSpec.getName(), macroEvaluator);
    }

    DataTracer dataTracer = sec.getDataTracer(stageSpec.getName());
    StreamingContext sourceContext = new DefaultStreamingContext(stageSpec, sec, streamingContext);
    JavaDStream<Object> javaDStream = source.getStream(sourceContext);
    if (dataTracer.isEnabled()) {
      // it will create a new function for each RDD, which would limit each RDD but not the entire DStream.
      javaDStream = javaDStream.transform(new LimitingFunction<>(spec.getNumOfRecordsPreview()));
    }
    JavaDStream<RecordInfo<Object>> outputDStream = javaDStream
      .transform(new CountingTransformFunction<>(stageSpec.getName(), sec.getMetrics(), "records.out", dataTracer))
      .map(new WrapOutputTransformFunction<>(stageSpec.getName()));
    return new DStreamCollection<>(sec, outputDStream);
  }

  @Override
  protected SparkPairCollection<Object, Object> addJoinKey(StageSpec stageSpec, String inputStageName,
                                                           SparkCollection<Object> inputCollection,
                                                           StageStatisticsCollector collector) throws Exception {
    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageSpec, sec, collector);
    JavaDStream<Object> dStream = inputCollection.getUnderlying();
    JavaPairDStream<Object, Object> result =
      dStream.transformToPair(new DynamicJoinOn<>(dynamicDriverContext, inputStageName));
    return new PairDStreamCollection<>(sec, result);
  }

  @Override
  protected SparkCollection<Object> mergeJoinResults(
    StageSpec stageSpec, SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs,
    StageStatisticsCollector collector) throws Exception {

    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageSpec, sec, collector);
    JavaPairDStream<Object, List<JoinElement<Object>>> pairDStream = joinedInputs.getUnderlying();
    JavaDStream<Object> result = pairDStream.transform(new DynamicJoinMerge<>(dynamicDriverContext));
    return new DStreamCollection<>(sec, result);
  }
}
