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

import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.connector.SingleConnectorFactory;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.SparkPipelineRunner;
import co.cask.cdap.etl.spark.SparkStageStatisticsCollector;
import co.cask.cdap.etl.spark.function.BatchSourceFunction;
import co.cask.cdap.etl.spark.function.JoinMergeFunction;
import co.cask.cdap.etl.spark.function.JoinOnFunction;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Batch Spark pipeline driver.
 */
public class BatchSparkPipelineDriver extends SparkPipelineRunner implements JavaSparkMain, TxRunnable {
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
  private transient int numOfRecordsPreview;

  @Override
  protected SparkCollection<RecordInfo<Object>> getSource(StageSpec stageSpec, StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return new RDDCollection<>(sec, jsc, datasetContext, sinkFactory,
                               sourceFactory.createRDD(sec, jsc, stageSpec.getName(), Object.class, Object.class)
                                 .flatMap(Compat.convert(new BatchSourceFunction(pluginFunctionContext,
                                                                                 numOfRecordsPreview))));
  }

  @Override
  protected SparkPairCollection<Object, Object> addJoinKey(StageSpec stageSpec, String inputStageName,
                                                           SparkCollection<Object> inputCollection,
                                                           StageStatisticsCollector collector) throws Exception {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return inputCollection.flatMapToPair(
      Compat.convert(new JoinOnFunction<>(pluginFunctionContext, inputStageName)));
  }

  @Override
  protected SparkCollection<Object> mergeJoinResults(
    StageSpec stageSpec,
    SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs,
    StageStatisticsCollector collector) throws Exception {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return joinedInputs.flatMap(Compat.convert(new JoinMergeFunction<>(pluginFunctionContext)));
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    this.jsc = new JavaSparkContext();
    this.sec = sec;

    // Execution the whole pipeline in one long transaction. This is because the Spark execution
    // currently share the same contract and API as the MapReduce one.
    // The API need to expose DatasetContext, hence it needs to be executed inside a transaction
    //Transactionals.execute(sec, this, Exception.class);
    run(datasetContext);
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
    numOfRecordsPreview = phaseSpec.getNumOfRecordsPreview();
    PipelinePluginContext pluginContext = new PipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                    phaseSpec.isStageLoggingEnabled(),
                                                                    phaseSpec.isProcessTimingEnabled());

    Map<String, StageStatisticsCollector> collectors = new HashMap<>();
    if (phaseSpec.pipelineContainsCondition()) {
      Iterator<StageSpec> iterator = phaseSpec.getPhase().iterator();
      while (iterator.hasNext()) {
        StageSpec spec = iterator.next();
        collectors.put(spec.getName(), new SparkStageStatisticsCollector(jsc));
      }
    }
    try {
      PipelinePluginInstantiator pluginInstantiator =
        new PipelinePluginInstantiator(pluginContext, sec.getMetrics(), phaseSpec, new SingleConnectorFactory());
      runPipeline(phaseSpec.getPhase(), BatchSource.PLUGIN_TYPE, sec, stagePartitions, pluginInstantiator, collectors);
    } finally {
      updateWorkflowToken(sec.getWorkflowToken(), collectors);
    }
  }

  private void updateWorkflowToken(WorkflowToken token, Map<String, StageStatisticsCollector> collectors) {
    for (Map.Entry<String, StageStatisticsCollector> entry : collectors.entrySet()) {
      SparkStageStatisticsCollector collector = (SparkStageStatisticsCollector) entry.getValue();
      String keyPrefix = Constants.StageStatistics.PREFIX + "." + entry.getKey() + ".";

      String inputRecordKey = keyPrefix + Constants.StageStatistics.INPUT_RECORDS;
      token.put(inputRecordKey, String.valueOf(collector.getInputRecordCount()));

      String outputRecordKey = keyPrefix + Constants.StageStatistics.OUTPUT_RECORDS;
      token.put(outputRecordKey, String.valueOf(collector.getOutputRecordCount()));

      String errorRecordKey = keyPrefix + Constants.StageStatistics.ERROR_RECORDS;
      token.put(errorRecordKey, String.valueOf(collector.getErrorRecordCount()));
    }
  }
}
