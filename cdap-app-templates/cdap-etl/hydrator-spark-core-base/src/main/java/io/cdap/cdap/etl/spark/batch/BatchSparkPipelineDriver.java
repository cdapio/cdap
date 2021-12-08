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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.batch.PipelinePluginInstantiator;
import io.cdap.cdap.etl.batch.connector.SingleConnectorFactory;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.SetMultimapCodec;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.engine.SQLEngineJob;
import io.cdap.cdap.etl.engine.SQLEngineUtils;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkCollectionRelationalEngine;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.SparkPipelineRunner;
import io.cdap.cdap.etl.spark.SparkStageStatisticsCollector;
import io.cdap.cdap.etl.spark.function.BatchSourceFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.JoinMergeFunction;
import io.cdap.cdap.etl.spark.function.JoinOnFunction;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch Spark pipeline driver.
 */
public class BatchSparkPipelineDriver extends SparkPipelineRunner implements JavaSparkMain, TxRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSparkPipelineDriver.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
    .registerTypeAdapter(InputFormatProvider.class, new InputFormatProviderTypeAdapter())
    .create();

  private transient JavaSparkContext jsc;
  private transient JavaSparkExecutionContext sec;
  private transient SparkBatchSourceFactory sourceFactory;
  private transient SparkBatchSinkFactory sinkFactory;
  private transient DatasetContext datasetContext;
  private transient Map<String, Integer> stagePartitions;
  private transient FunctionCache.Factory functionCacheFactory;
  private transient BatchSQLEngineAdapter sqlEngineAdapter = null;

  /**
   * Empty constructor, used when instantiating this class.
   */
  public BatchSparkPipelineDriver() {
  }

  /**
   * Only used during unit testing.
   */
  @VisibleForTesting
  protected BatchSparkPipelineDriver(BatchSQLEngineAdapter sqlEngineAdapter) {
    this.sqlEngineAdapter = sqlEngineAdapter;
  }

  @Override
  protected SparkCollection<RecordInfo<Object>> getSource(StageSpec stageSpec,
                                                          FunctionCache.Factory functionCacheFactory,
                                                          StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    FlatMapFunction<Tuple2<Object, Object>, RecordInfo<Object>> sourceFunction =
      new BatchSourceFunction(pluginFunctionContext, functionCacheFactory.newCache());
    this.functionCacheFactory = functionCacheFactory;
    return new RDDCollection<>(sec, functionCacheFactory, jsc,
                               new SQLContext(jsc), datasetContext, sinkFactory, sourceFactory
      .createRDD(sec, jsc, stageSpec.getName(), Object.class, Object.class)
      .flatMap(sourceFunction)
    );
  }

  @Override
  protected SparkPairCollection<Object, Object> addJoinKey(StageSpec stageSpec,
                                                           FunctionCache.Factory functionCacheFactory,
                                                           String inputStageName,
                                                           SparkCollection<Object> inputCollection,
                                                           StageStatisticsCollector collector) throws Exception {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return inputCollection.flatMapToPair(
      new JoinOnFunction<>(pluginFunctionContext, functionCacheFactory.newCache(), inputStageName));
  }

  @Override
  protected SparkCollection<Object> mergeJoinResults(
    StageSpec stageSpec,
    FunctionCache.Factory functionCacheFactory,
    SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs,
    StageStatisticsCollector collector) throws Exception {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return joinedInputs.flatMap(new JoinMergeFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache()));
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    this.jsc = new JavaSparkContext();
    this.sec = sec;

    // Execution the whole pipeline in one long transaction. This is because the Spark execution
    // currently share the same contract and API as the MapReduce one.
    // The API need to expose DatasetContext, hence it needs to be executed inside a transaction
    Transactionals.execute(sec, this, Exception.class);
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

    boolean isSuccessful = true;

    try {
      PipelinePluginInstantiator pluginInstantiator =
        new PipelinePluginInstantiator(pluginContext, sec.getMetrics(), phaseSpec, new SingleConnectorFactory());
      boolean shouldConsolidateStages = Boolean.parseBoolean(
        sec.getRuntimeArguments().getOrDefault(Constants.CONSOLIDATE_STAGES, Boolean.TRUE.toString()));
      boolean shouldCacheFunctions = Boolean.parseBoolean(
        sec.getRuntimeArguments().getOrDefault(Constants.CACHE_FUNCTIONS, Boolean.TRUE.toString()));
      boolean isPreviewEnabled =
        phaseSpec.getPhase().size() == 0
          || sec.getDataTracer(phaseSpec.getPhase().iterator().next().getName()).isEnabled();

      // Initialize SQL engine instance if needed.
      if (!isPreviewEnabled && phaseSpec.getSQLEngineStageSpec() != null) {
        String sqlEngineStage = SQLEngineUtils.buildStageName(phaseSpec.getSQLEngineStageSpec().getPlugin().getName());

        // Instantiate SQL engine and prepare run.
        try {
          MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(sec),
                                                                    sec.getLogicalStartTime(),
                                                                    sec.getSecureStore(),
                                                                    sec.getServiceDiscoverer(),
                                                                    sec.getNamespace());
          Object instance = pluginInstantiator.newPluginInstance(sqlEngineStage,
                                                                 macroEvaluator);
          sqlEngineAdapter = new BatchSQLEngineAdapter((SQLEngine<?, ?, ?, ?>) instance,
                                                       sec,
                                                       jsc,
                                                       collectors);
          sqlEngineAdapter.prepareRun();
        } catch (InstantiationException ie) {
          LOG.error("Could not create plugin instance for SQLEngine class", ie);
        } finally {
          if (sqlEngineAdapter == null) {
            LOG.warn("Could not instantiate SQLEngine instance for Transformation Pushdown");
          }
        }
      }

      runPipeline(phaseSpec, BatchSource.PLUGIN_TYPE, sec, stagePartitions, pluginInstantiator, collectors,
                  sinkFactory.getUncombinableSinks(), shouldConsolidateStages, shouldCacheFunctions);
    } catch (Throwable t) {
      // Mark this execution as not successful.
      isSuccessful = false;

      // Rethrow
      throw t;
    } finally {
      updateWorkflowToken(sec.getWorkflowToken(), collectors);

      // Close SQL Engine Adapter if neeeded,
      if (sqlEngineAdapter != null) {
        sqlEngineAdapter.onRunFinish(isSuccessful);
        sqlEngineAdapter.close();
      }
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

  @Override
  @SuppressWarnings("unchecked")
  protected SparkCollection<Object> handleAutoJoin(String stageName, JoinDefinition joinDefinition,
                                                   Map<String, SparkCollection<Object>> inputDataCollections,
                                                   @Nullable Integer numPartitions) {

    if (sqlEngineAdapter != null && canJoinOnSQLEngine(stageName, joinDefinition, inputDataCollections)) {
      // If we can execute this join operation using the SQL engine, we need to replace all Input collections with
      // collections representing data that has been pushed to the SQL engine.
      for (JoinStage joinStage : joinDefinition.getStages()) {
        String joinStageName = joinStage.getStageName();

        // If the input collection is already a SQL Engine collection, there's no need to push.
        if (inputDataCollections.get(joinStageName) instanceof SQLBackedCollection) {
          continue;
        }

        SparkCollection<Object> collection = inputDataCollections.get(joinStage.getStageName());

        SQLEngineJob<SQLDataset> pushJob = sqlEngineAdapter.push(joinStageName,
                                                                 joinStage.getSchema(),
                                                                 collection);
        inputDataCollections.put(joinStageName,
                                 new SQLEngineCollection<>(sec, functionCacheFactory, jsc, new SQLContext(jsc),
                                                           datasetContext, sinkFactory, collection,
                                                           joinStageName, sqlEngineAdapter, pushJob));
      }
    }

    return super.handleAutoJoin(stageName, joinDefinition, inputDataCollections, numPartitions);
  }

  /**
   * Decide if we should pushdown this join operation into the SQL Engine.
   *
   * We will use pushdown if a SQL engine is available, unless:
   *
   * 1. One of the sides of the join is a broadcast, unle
   *
   * @param stageName the name of the Stage
   * @param joinDefinition the Join Definition
   * @param inputDataCollections the input data collections
   * @return boolean used to decide wether to pushdown this collection or not.
   */
  protected boolean canJoinOnSQLEngine(String stageName,
                                       JoinDefinition joinDefinition,
                                       Map<String, SparkCollection<Object>> inputDataCollections) {
    // Check if the SQL engine is able to execute this join definition.
    // If not supported, the Join will be executed in Spark.
    if (!sqlEngineAdapter.canJoin(stageName, joinDefinition)) {
      return false;
    }

    boolean containsBroadcastStage = false;

    for (JoinStage stage : joinDefinition.getStages()) {
      if (stage.isBroadcast()) {
        // If there is a broadcast stage in this join, this is a potential reason to not execute the join in the SQL
        // engine.
        containsBroadcastStage = true;
      } else if (inputDataCollections.get(stage.getStageName()) instanceof SQLBackedCollection) {
        // If any of the existing non-broadcast input collections already exists on the SQL engine, we should
        // execute this operation in the SQL engine.
        return true;
      }
    }

    // Execute in the SQL engine if there are no broadcast stages for this join.
    return !containsBroadcastStage;
  }

  /**
   * If SQL Engine is present, supports relational transform and current stage data is already
   * provided by SQL engine, adds SQL Engine implementation of relational engine
   * @param stageData
   * @return
   */
  @Override
  protected Iterable<SparkCollectionRelationalEngine> getRelationalEngines(SparkCollection<Object> stageData) {
    if (sqlEngineAdapter == null || !sqlEngineAdapter.supportsRelationalTranform()
      || !(stageData instanceof SQLBackedCollection)) {
      //Relational transform on SQL engine is not supported
      return super.getRelationalEngines(stageData);
    }
    SQLEngineRelationalEngine relationalEngine = new SQLEngineRelationalEngine(
      sec, functionCacheFactory, jsc, new SQLContext(jsc), datasetContext, sinkFactory, sqlEngineAdapter);
    return Iterables.concat(
      Collections.singletonList(relationalEngine),
      super.getRelationalEngines(stageData)
    );
  }
}
