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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.SparkPipelineRunner;
import co.cask.cdap.etl.spark.function.BatchSourceFunction;
import co.cask.cdap.etl.spark.function.JoinMergeFunction;
import co.cask.cdap.etl.spark.function.JoinOnFunction;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Batch Spark pipeline driver.
 */
public class BatchSparkPipelineDriver extends SparkPipelineRunner
  implements JavaSparkMain, TxRunnable {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
    .registerTypeAdapter(OutputFormatProvider.class, new OutputFormatProviderTypeAdapter())
    .registerTypeAdapter(InputFormatProvider.class, new InputFormatProviderTypeAdapter())
    .create();

  private transient JavaSparkContext jsc;
  private transient JavaSparkExecutionContext sec;
  private transient SparkBatchSourceFactory sourceFactory;
  private transient SparkBatchSinkFactory sinkFactory;
  private transient DatasetContext datasetContext;
  private transient Map<String, Integer> stagePartitions;

  @Override
  protected SparkCollection<Object> getSource(StageInfo stageInfo) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageInfo, sec);
    return new RDDCollection<>(sec, jsc, datasetContext, sinkFactory,
                               sourceFactory.createRDD(sec, jsc, stageInfo.getName(), Object.class, Object.class)
                                 .flatMap(new BatchSourceFunction(pluginFunctionContext)));
  }

  @Override
  protected SparkPairCollection<Object, Object> addJoinKey(StageInfo stageInfo, String inputStageName,
                                                           SparkCollection<Object> inputCollection) throws Exception {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageInfo, sec);
    return inputCollection.flatMapToPair(new JoinOnFunction<>(pluginFunctionContext, inputStageName));
  }

  @Override
  protected SparkCollection<Object> mergeJoinResults(
    StageInfo stageInfo,
    SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs) throws Exception {

    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageInfo, sec);
    return joinedInputs.flatMap(new JoinMergeFunction<>(pluginFunctionContext));
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    this.jsc = new JavaSparkContext();
    this.sec = sec;

    // Execution the whole pipeline in one long transaction. This is because the Spark execution
    // currently share the same contract and API as the MapReduce one.
    // The API need to expose DatasetContext, hence it needs to be executed inside a transaction
    sec.execute(this);
  }

  @Override
  public void run(DatasetContext context) throws Exception {
    BatchPhaseSpec phaseSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
                                             BatchPhaseSpec.class);

    Path configFile = sec.getLocalizationContext().getLocalFile("HydratorSpark.config").toPath();
    try (BufferedReader reader = Files.newBufferedReader(configFile, StandardCharsets.UTF_8)) {
      String object = reader.readLine();
      SparkBatchSourceSinkFactoryInfo sourceSinkInfo = GSON.fromJson(object, SparkBatchSourceSinkFactoryInfo.class);
      sourceFactory = sourceSinkInfo.getSparkBatchSourceFactory();
      sinkFactory = sourceSinkInfo.getSparkBatchSinkFactory();
      stagePartitions = sourceSinkInfo.getStagePartitions();
    }
    datasetContext = context;
    runPipeline(phaseSpec.getPhase(), BatchSource.PLUGIN_TYPE, sec, stagePartitions);
  }
}
