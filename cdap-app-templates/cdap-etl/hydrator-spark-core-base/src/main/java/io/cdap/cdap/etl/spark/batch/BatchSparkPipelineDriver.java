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
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.WrappedException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineInput;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.relational.Engine;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
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
  private transient BatchSQLEngineAdapter sqlEngineAdapter;
  private transient BatchSQLEngineAdapter fallbackSqlEngineAdapter;

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
    // Initialize function cache factory
    this.functionCacheFactory = functionCacheFactory;

    String sourceStageName = stageSpec.getName();

    // Check if the SQL Engine is inialized and look for compatible sources
    if (sqlEngineAdapter != null) {
      // If the SQL Engine is initialized, and the stage is a compatible Input stage for this SQL engine, return a
      // SQLBacked Collection which will try to execute the SQL Input operation and fail the pipeline in case of any
      // sql failure
      if (sourceFactory.getSQLEngineInput(sourceStageName, sqlEngineAdapter.getSQLEngineClassName()) != null) {
        LOG.info("Source stage {} is compatible with SQL Engine.", sourceStageName);
        SQLEngineInput sourceSQLEngineInput = sourceFactory.getSQLEngineInput(sourceStageName,
                                                                              sqlEngineAdapter.getSQLEngineClassName());

        return getSourceSQLBackedCollection(sourceStageName, sourceSQLEngineInput);
      } else {
        LOG.debug("Source stage {} is not compatible with SQL Engine.", sourceStageName);
      }
    }

    // If SQL engine is not initiated : use default spark method (RDDCollection or OpaqueDatasetCollection)
    boolean shouldForceDatasets = Boolean.parseBoolean(
        sec.getRuntimeArguments().getOrDefault(Constants.DATASET_FORCE, Boolean.FALSE.toString()));
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    FlatMapFunction<Tuple2<Object, Object>, RecordInfo<Object>> sourceFunction =
      new BatchSourceFunction(pluginFunctionContext, functionCacheFactory.newCache());
    this.functionCacheFactory = functionCacheFactory;
    try {
      JavaRDD<RecordInfo<Object>> rdd = sourceFactory
          .createRDD(sec, jsc, stageSpec.getName(), Object.class, Object.class)
          .flatMap(sourceFunction);
      if (shouldForceDatasets) {
        return OpaqueDatasetCollection.fromRdd(
            rdd, sec, jsc, new SQLContext(jsc), datasetContext, sinkFactory, functionCacheFactory);
      }
      return new RDDCollection<>(sec, functionCacheFactory, jsc,
          new SQLContext(jsc), datasetContext, sinkFactory, rdd);
    } catch (Exception e) {
      List<Throwable> causalChain = Throwables.getCausalChain(e);
      for(Throwable cause : causalChain) {
        if (cause instanceof WrappedException) {
          String stageName = ((WrappedException) cause).getStageName();
          LOG.error("Stage: {}", stageName);
          MDC.put("Failed_Stage", stageName);
          throw new WrappedException(e, stageName);
        }
      }
      MDC.put("Failed_Stage", stageSpec.getName());
      throw new WrappedException(e, stageSpec.getName());
    }
  }

  @Override
  protected JavaSparkContext getSparkContext() {
    return jsc;
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
    boolean isPreviewEnabled = phaseSpec.isPreviewEnabled(sec);

    try {
      PipelinePluginInstantiator pluginInstantiator =
        new PipelinePluginInstantiator(pluginContext, sec.getMetrics(), phaseSpec, new SingleConnectorFactory());
      boolean shouldConsolidateStages = Boolean.parseBoolean(
        sec.getRuntimeArguments().getOrDefault(Constants.CONSOLIDATE_STAGES, Boolean.TRUE.toString()));
      boolean shouldCacheFunctions = Boolean.parseBoolean(
        sec.getRuntimeArguments().getOrDefault(Constants.CACHE_FUNCTIONS, Boolean.TRUE.toString()));
      boolean shouldDisablePushdown = Boolean.parseBoolean(
        sec.getRuntimeArguments().getOrDefault(Constants.DISABLE_ELT_PUSHDOWN, Boolean.FALSE.toString()));

      // Initialize SQL engine instance if needed.
      if (!isPreviewEnabled && phaseSpec.getSQLEngineStageSpec() != null && !shouldDisablePushdown) {
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
          sqlEngineAdapter = new BatchSQLEngineAdapter(phaseSpec.getSQLEngineStageSpec().getPlugin().getName(),
                                                       (SQLEngine<?, ?, ?, ?>) instance,
                                                       sec,
                                                       jsc,
                                                       collectors,
                                                       isPreviewEnabled);
          sqlEngineAdapter.prepareRun();
        } catch (InstantiationException ie) {
          LOG.error("Could not create plugin instance for SQLEngine class", ie);
        } finally {
          if (sqlEngineAdapter == null) {
            LOG.warn("Could not instantiate SQLEngine instance for Transformation Pushdown");
          }
        }
      }

      fallbackSqlEngineAdapter = new BatchSQLEngineAdapter("SPARKSQL",
                                                     new SparkSQLEngine(),
                                                     sec,
                                                     jsc,
                                                     collectors,
                                                     isPreviewEnabled,
                                                     true);

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

      if (fallbackSqlEngineAdapter != null) {
        fallbackSqlEngineAdapter.onRunFinish(isSuccessful);
        fallbackSqlEngineAdapter.close();
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

        BatchCollection<Object> collection =
            (BatchCollection<Object>) inputDataCollections.get(joinStage.getStageName());

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
   * <p>
   * We will use pushdown if a SQL engine is available, unless:
   * <p>
   * 1. One of the sides of the join is a broadcast, unle
   *
   * @param stageName            the name of the Stage
   * @param joinDefinition       the Join Definition
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

    // Explicitly skip this stage if the stage is configured as an excluded stage.
    if (shouldForceSkipSQLEngine(stageName)) {
      return false;
    }

    // Explicitly include this stage if the stage is configured as an included stage.
    if (shouldForcePushToSQLEngine(stageName)) {
      return true;
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
   * Check if this stage is configured as a stage that should be pushed to the SQL engine
   *
   * @param stageName stage name
   * @return boolean stating if this stage should always try to be executed in the SQL engine.
   */
  protected boolean shouldForcePushToSQLEngine(String stageName) {
    return sqlEngineAdapter != null && sqlEngineAdapter.getIncludedStageNames().contains(stageName);
  }

  /**
   * Check if this stage is configured as a stage that should never be pushed to the SQL engine
   *
   * @param stageName stage name
   * @return boolean stating if this stage should always be skipped from executing in the SQL engine.
   */
  protected boolean shouldForceSkipSQLEngine(String stageName) {
    return sqlEngineAdapter != null && sqlEngineAdapter.getExcludedStageNames().contains(stageName);
  }

  /**
   * If SQL Engine is present, supports relational transform and current stage data is already provided by SQL engine,
   * adds SQL Engine implementation of relational engine
   *
   * @param stageData
   * @return
   */
  @Override
  protected Iterable<SparkCollectionRelationalEngine> getRelationalEngines(StageSpec stageSpec,
                                                                           SparkCollection<Object> stageData) {
    if (sqlEngineAdapter == null || !sqlEngineAdapter.supportsRelationalTranform()) {
      //Relational transform on SQL engine is not supported
      return generateSQLRelationEngines(fallbackSqlEngineAdapter, stageSpec, stageData);
    }

    // Explicitly skip this stage if the stage is configured as an excluded stage.
    if (shouldForceSkipSQLEngine(stageSpec.getName())) {
      return generateSQLRelationEngines(fallbackSqlEngineAdapter, stageSpec, stageData);
    }

    // If this stage is not pushed down and it's not in the included stages and is NOT a local engine,
    // we can skip relational transformation in the SQL engine.
    if (!(stageData instanceof SQLBackedCollection) && !shouldForcePushToSQLEngine(stageSpec.getName())) {
      return generateSQLRelationEngines(fallbackSqlEngineAdapter, stageSpec, stageData);
    }
    return generateSQLRelationEngines(sqlEngineAdapter, stageSpec, stageData);
  }

  private Iterable<SparkCollectionRelationalEngine> generateSQLRelationEngines(BatchSQLEngineAdapter sqlEngineAdapter,
                                                                               StageSpec stageSpec,
                                                                               SparkCollection<Object> stageData) {
    SQLEngineRelationalEngine relationalEngine = new SQLEngineRelationalEngine(
      sec, functionCacheFactory, jsc, new SQLContext(jsc), datasetContext, sinkFactory, sqlEngineAdapter);
    return Iterables.concat(
      Collections.singletonList(relationalEngine),
      super.getRelationalEngines(stageSpec, stageData)
    );
  }

  public Engine getSQLRelationalEngine() {
    return sqlEngineAdapter.getSQLRelationalEngine();
  }

  /**
   * Contains logic to read a {@link SQLEngineInput} using the SQL engine adapter.
   * @param stageName Stage name to read
   * @param input SQL Input specification
   * @return a {@link SQLBackedCollection} representing the records from this SQL input
   */
  protected SQLBackedCollection<RecordInfo<Object>> getSourceSQLBackedCollection(String stageName,
                                                                                 SQLEngineInput input) {
    // Execute read operation using the stage input.
    SQLEngineJob<SQLDataset> readJob = sqlEngineAdapter.read(stageName, input);
    SQLEngineCollection<Object> sqlCollection =
      new SQLEngineCollection<>(sec, functionCacheFactory, jsc, new SQLContext(jsc), datasetContext,
                                sinkFactory, stageName, sqlEngineAdapter,  readJob);
    return (SQLBackedCollection<RecordInfo<Object>>) mapToRecordInfoCollection(stageName, sqlCollection);
  }
}
