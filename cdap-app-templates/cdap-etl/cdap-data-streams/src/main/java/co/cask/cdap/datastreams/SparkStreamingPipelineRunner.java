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
import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.SparkPipelineRunner;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.streaming.DStreamCollection;
import co.cask.cdap.etl.spark.streaming.DefaultStreamingContext;
import co.cask.cdap.etl.spark.streaming.DynamicDriverContext;
import co.cask.cdap.etl.spark.streaming.PairDStreamCollection;
import co.cask.cdap.etl.spark.streaming.function.CountingTranformFunction;
import co.cask.cdap.etl.spark.streaming.function.DynamicJoinMerge;
import co.cask.cdap.etl.spark.streaming.function.DynamicJoinOn;
import co.cask.cdap.etl.spark.streaming.function.preview.LimitingFunction;
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
  private final boolean checkpointsDisabled;
  private final int numOfRecordsPreview;

  public SparkStreamingPipelineRunner(JavaSparkExecutionContext sec, JavaStreamingContext streamingContext,
                                      boolean checkpointsDisabled, int numOfRecordsPreview) {
    this.sec = sec;
    this.streamingContext = streamingContext;
    this.checkpointsDisabled = checkpointsDisabled;
    this.numOfRecordsPreview = numOfRecordsPreview;
  }

  @Override
  protected SparkCollection<Object> getSource(StageInfo stageInfo) throws Exception {
    StreamingSource<Object> source;
    if (checkpointsDisabled) {
      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageInfo, sec);
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
      source = sec.getPluginContext().newPluginInstance(stageInfo.getName(), macroEvaluator);
    }

    DataTracer dataTracer = sec.getDataTracer(stageInfo.getName());
    StreamingContext sourceContext = new DefaultStreamingContext(stageInfo, sec, streamingContext);
    JavaDStream<Object> javaDStream = source.getStream(sourceContext);
    if (dataTracer.isEnabled()) {
      // it will create a new function for each RDD, which would limit each RDD but not the entire DStream.
      javaDStream = javaDStream.transform(new LimitingFunction<>(numOfRecordsPreview));
    }
    javaDStream = javaDStream.transform(new CountingTranformFunction<>(stageInfo.getName(), sec.getMetrics(),
                                                                       "records.out", dataTracer));
    return new DStreamCollection<>(sec, javaDStream);
  }

  @Override
  protected SparkPairCollection<Object, Object> addJoinKey(StageInfo stageInfo, String inputStageName,
                                                           SparkCollection<Object> inputCollection) throws Exception {
    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageInfo, sec);
    JavaDStream<Object> dStream = inputCollection.getUnderlying();
    JavaPairDStream<Object, Object> result =
      dStream.transformToPair(new DynamicJoinOn<>(dynamicDriverContext, inputStageName));
    return new PairDStreamCollection<>(sec, result);
  }

  @Override
  protected SparkCollection<Object> mergeJoinResults(
    StageInfo stageInfo, SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs) throws Exception {

    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageInfo, sec);
    JavaPairDStream<Object, List<JoinElement<Object>>> pairDStream = joinedInputs.getUnderlying();
    JavaDStream<Object> result = pairDStream.transform(new DynamicJoinMerge<>(dynamicDriverContext));
    return new DStreamCollection<>(sec, result);
  }
}
