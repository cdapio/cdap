/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.common.base.Objects;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.capability.PullCapability;
import io.cdap.cdap.etl.api.engine.sql.capability.PushCapability;
import io.cdap.cdap.etl.api.engine.sql.dataset.RecordCollection;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetConsumer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetProducer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLRelationDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformDefinition;
import io.cdap.cdap.etl.api.engine.sql.request.SQLTransformRequest;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTransform;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollection;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollectionImpl;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.engine.SQLEngineJob;
import io.cdap.cdap.etl.engine.SQLEngineJobKey;
import io.cdap.cdap.etl.engine.SQLEngineJobType;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.function.TransformFromPairFunction;
import io.cdap.cdap.etl.spark.function.TransformToPairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Adapter used to orchestrate interaction between the Pipeline Runner and the SQL Engine.
 */
public class BatchSQLEngineAdapter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSQLEngineAdapter.class);

  private final JavaSparkExecutionContext sec;
  private final JavaSparkContext jsc;
  private final SQLContext sqlContext;
  private final SQLEngine<?, ?, ?, ?> sqlEngine;
  private final Metrics metrics;
  private final Map<String, StageStatisticsCollector> statsCollectors;
  private final ExecutorService executorService =
    Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("batch-sql-engine-adapter"));
  private final Map<SQLEngineJobKey, SQLEngineJob<?>> jobs;

  public BatchSQLEngineAdapter(SQLEngine<?, ?, ?, ?> sqlEngine,
                               JavaSparkExecutionContext sec,
                               JavaSparkContext jsc,
                               Map<String, StageStatisticsCollector> statsCollectors) {
    this.sqlEngine = sqlEngine;
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = new SQLContext(jsc);
    this.metrics = sec.getMetrics();
    this.statsCollectors = statsCollectors;
    this.jobs = new HashMap<>();
  }

  /**
   * Call the SQLEngine PrepareRun method
   * @throws Exception if the underlying prepareRun call fails.
   */
  public void prepareRun() throws Exception {
    sqlEngine.prepareRun(sec);
  }

  /**
   * Call the SQLEngine onRunFinish method
   * @throws Exception if the underlying onRunFinish call fails.
   */
  public void onRunFinish(boolean succeeded) throws Exception {
    sqlEngine.onRunFinish(succeeded, sec);
  }

  /**
   * Creates a new job tu push a SparkCollection into the SQL engine.
   *
   * @param datasetName the name of the dataset to push
   * @param schema      the schema for this dataset
   * @param collection  the Spark collection containing the dataset to push
   * @return Job representing this Push operation.
   */
  @SuppressWarnings("unchecked,raw")
  protected SQLEngineJob<SQLDataset> push(String datasetName,
                                          Schema schema,
                                          SparkCollection<?> collection) {
    //If this job already exists, return the existing instance.
    SQLEngineJobKey jobKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.PUSH);
    if (jobs.containsKey(jobKey)) {
      return (SQLEngineJob<SQLDataset>) jobs.get(jobKey);
    }

    CompletableFuture<SQLDataset> future = new CompletableFuture<>();

    Runnable pushTask = () -> {
      try {
        LOG.debug("Starting push for dataset '{}'", datasetName);
        SQLDataset result = pushInternal(datasetName, schema, collection);
        LOG.debug("Completed push for dataset '{}'", datasetName);
        future.complete(result);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    };

    executorService.submit(pushTask);

    SQLEngineJob<SQLDataset> job = new SQLEngineJob<>(jobKey, future);
    jobs.put(jobKey, job);

    return job;
  }

  /**
   * Push implementation. This method has blocking calls and should be executed in a separate thread.
   *
   * @param datasetName name of the dataset to push.
   * @param schema      the record schema.
   * @param collection  the collection containing the records to push.
   * @return {@link SQLDataset} instance representing the pushed records.
   * @throws SQLEngineException if the push operation fails.
   */
  @SuppressWarnings("unchecked")
  public SQLDataset pushInternal(String datasetName,
                                 Schema schema,
                                 SparkCollection<?> collection) throws SQLEngineException {
    // Create push request
    SQLPushRequest pushRequest = new SQLPushRequest(datasetName, schema);

    // Check if any of the declared capabilities for this plugin is able to consume this Push Request.
    // If so, we will process this request using a consumer.
    for (PushCapability capability : sqlEngine.getPushCapabilities()) {
      SQLDatasetConsumer consumer = sqlEngine.getConsumer(pushRequest, capability);

      // If a consumer is able to consume this request, we delegate the execution to the consumer.
      if (consumer != null) {
        StructType sparkSchema = DataFrames.toDataType(schema);
        JavaRDD<Row> rowRDD = ((JavaRDD<StructuredRecord>) collection.getUnderlying())
          .map(r -> DataFrames.toRow(r, sparkSchema));
        Dataset<Row> ds = sqlContext.createDataFrame(rowRDD, sparkSchema);
        RecordCollection recordCollection = new SparkRecordCollectionImpl(ds);
        return consumer.consume(recordCollection);
      }
    }

    // If no capabilities could be used to produce records, proceed using the Push Provider.
    SQLPushDataset<StructuredRecord, ?, ?> pushDataset = sqlEngine.getPushProvider(pushRequest);

    //Write records using the Push provider.
    JavaPairRDD<?, ?> pairRdd =
      ((JavaRDD) collection.getUnderlying()).flatMapToPair(new TransformToPairFunction<>(pushDataset.toKeyValue()));
    RDDUtils.saveUsingOutputFormat(pushDataset, pairRdd);
    return pushDataset;
  }

  /**
   * Creates a new job to pull a Spark Collection from the SQL engine
   *
   * @param job the job representing the compute stage for the dataset we need to pull.
   * @return Job representing this pull operation.
   */
  @SuppressWarnings("unchecked,raw")
  public <T> SQLEngineJob<JavaRDD<T>> pull(SQLEngineJob<SQLDataset> job) {
    //If this job already exists, return the existing instance.
    SQLEngineJobKey jobKey = new SQLEngineJobKey(job.getDatasetName(), SQLEngineJobType.PULL);
    if (jobs.containsKey(jobKey)) {
      return (SQLEngineJob<JavaRDD<T>>) jobs.get(jobKey);
    }

    CompletableFuture<JavaRDD<T>> future = new CompletableFuture<>();

    Runnable pullTask = () -> {
      try {
        LOG.debug("Starting pull for dataset '{}'", job.getDatasetName());
        waitForJobAndHandleExceptionInternal(job);
        JavaRDD<T> result = pullInternal(job.waitFor());
        LOG.debug("Completed pull for dataset '{}'", job.getDatasetName());
        future.complete(result);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    };

    executorService.submit(pullTask);

    SQLEngineJob<JavaRDD<T>> pullJob = new SQLEngineJob<>(jobKey, future);
    jobs.put(jobKey, pullJob);

    return pullJob;
  }

  /**
   * Pull implementation. This method has blocking calls and should be executed in a separate thread.
   *
   * @param dataset the dataset to pull.
   * @return {@link JavaRDD} representing the records contained in this dataset.
   * @throws SQLEngineException if the pull process fails.
   */
  @SuppressWarnings("unchecked,raw")
  private <T> JavaRDD<T> pullInternal(SQLDataset dataset) throws SQLEngineException {
    // Create pull operation for this dataset and wait until completion
    SQLPullRequest pullRequest = new SQLPullRequest(dataset);

    // Check if any of the declared capabilities for this plugin is able to produce records using this Pull Request.
    // If so, we will process this request using a producer.
    for (PullCapability capability : sqlEngine.getPullCapabilities()) {
      SQLDatasetProducer producer = sqlEngine.getProducer(pullRequest, capability);

      // If a producer is able to produce records for this pull request, extract the RDD from this request.
      if (producer != null) {
        RecordCollection recordCollection = producer.produce(dataset);

        // Note that we only support Spark collections at this time.
        // If the collection that got generarted is not an instance of a SparkRecordCollection, skip.
        if (recordCollection instanceof SparkRecordCollection) {
          Schema schema = dataset.getSchema();
          return (JavaRDD<T>) ((SparkRecordCollection) recordCollection).getDataFrame()
            .javaRDD()
            .map(r -> DataFrames.fromRow((Row) r, schema));
        }
      }
    }

    // If no capabilities could be used to produce records, proceed using the Pull Provider.
    SQLPullDataset<StructuredRecord, ?, ?> sqlPullDataset = sqlEngine.getPullProvider(pullRequest);

    // Run operation to read from the InputFormatProvider supplied by this operation.
    ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   getClass().getClassLoader());
    JavaPairRDD pairRDD = RDDUtils.readUsingInputFormat(jsc, sqlPullDataset, classLoader, Object.class,
                                                        Object.class);
    return pairRDD.flatMap(new TransformFromPairFunction(sqlPullDataset.fromKeyValue()));
  }

  /**
   * Check if a collection exists in the SQL Engine.
   * <p>
   * If there are already jobs in place to either push or compute this dataset, we will assume that this job exists on
   * the engine.
   * <p>
   * Otherwise, we delegate to the SQL engine.
   *
   * @param datasetName the name of the dataset to verify
   * @return boolean detailing if the collection exists or not.
   */
  public boolean exists(String datasetName) {
    SQLEngineJobKey joinStagePushKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.PUSH);
    if (jobs.containsKey(joinStagePushKey)) {
      return true;
    }

    SQLEngineJobKey joinStageExecKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.EXECUTE);
    if (jobs.containsKey(joinStageExecKey)) {
      return true;
    }

    return false;
  }

  /**
   * Verify if a Join Definition can be executed on a SQL Engine.
   *
   * @param datasetName    the dataset name to use to store the result of the join operation
   * @param joinDefinition the Join Definition
   * @return boolean specifying if this join operation can be executed on the SQL engine.
   */
  public boolean canJoin(String datasetName,
                         JoinDefinition joinDefinition) {
    SQLJoinDefinition sqlJoinDefinition = new SQLJoinDefinition(datasetName, joinDefinition);
    return sqlEngine.canJoin(sqlJoinDefinition);
  }

  /**
   *
   * @return if underlying engine support relational transform
   * @see SQLEngine#supportsRelationalTranform
   */
  public boolean supportsRelationalTranform() {
    return sqlEngine.supportsRelationalTranform();
  }

  /**
   *
   * @return relational engine provided by SQL Engine
   * @see SQLEngine#getRelationalEngine()
   */
  public Engine getSQLRelationalEngine() {
    return sqlEngine.getRelationalEngine();
  }

  /**
   * Executes a Join operation in the SQL engine
   *
   * @param datasetName    the dataset name to use to store the result of the join operation
   * @param joinDefinition the Join Definition
   * @return Job representing this join operation
   */
  @SuppressWarnings("unchecked,raw")
  public SQLEngineJob<SQLDataset> join(String datasetName,
                                       JoinDefinition joinDefinition) {
    return runJob(datasetName, () -> {
        Collection<SQLDataset> inputDatasets = getJoinInputDatasets(joinDefinition);
        SQLJoinRequest joinRequest = new SQLJoinRequest(datasetName, joinDefinition, inputDatasets);

        if (!sqlEngine.canJoin(joinRequest)) {
          throw new IllegalArgumentException("Unable to execute this join in the SQL engine");
        }

        return joinInternal(joinRequest);
      });
  }

  /**
   * Kicks off a {@link SQLEngineJobType.EXECUTE} job trat will produce a dataset
   * @param datasetName dataset name
   * @param jobFunction actual runnable that will do the work
   * @param <T> type of result
   * @return job that produces jobFunction result when finished
   */
  private <T> SQLEngineJob<T> runJob(String datasetName, Supplier<T> jobFunction) {
    //If this job already exists, return the existing instance.
    SQLEngineJobKey jobKey = new SQLEngineJobKey(datasetName, SQLEngineJobType.EXECUTE);
    if (jobs.containsKey(jobKey)) {
      return (SQLEngineJob<T>) jobs.get(jobKey);
    }

    CompletableFuture<T> future = new CompletableFuture<>();

    Runnable joinTask = () -> {
      try {
        LOG.debug("Starting job for dataset '{}'", datasetName);
        future.complete(jobFunction.get());
        LOG.debug("Completed job for dataset '{}'", datasetName);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    };

    executorService.submit(joinTask);

    SQLEngineJob<T> job = new SQLEngineJob<>(jobKey, future);
    jobs.put(jobKey, job);

    return job;
  }

  /**
   * Gets all datasets that are required to execute the supplied Join Definition.
   *
   * @param joinDefinition the join definition to check
   * @return Collection containing all the {@link SQLDataset} instances needed to execute this join.
   */
  @SuppressWarnings("unchecked")
  private Collection<SQLDataset> getJoinInputDatasets(JoinDefinition joinDefinition) throws IllegalArgumentException {
    List<SQLDataset> datasets = new ArrayList<>(joinDefinition.getStages().size());

    for (JoinStage stage : joinDefinition.getStages()) {
      datasets.add(getDatasetForStage(stage.getStageName()));
    }

    return datasets;
  }

  private SQLDataset getDatasetForStage(String stageName) {
    // Wait for the previous push or execute jobs to complete
    SQLEngineJobKey pushJobKey = new SQLEngineJobKey(stageName, SQLEngineJobType.PUSH);
    SQLEngineJobKey execJobKey = new SQLEngineJobKey(stageName, SQLEngineJobType.EXECUTE);

    if (jobs.containsKey(pushJobKey)) {
      SQLEngineJob<SQLDataset> job = (SQLEngineJob<SQLDataset>) jobs.get(pushJobKey);
      waitForJobAndHandleExceptionInternal(job);
      return job.waitFor();
    } else if (jobs.containsKey(execJobKey)) {
      SQLEngineJob<SQLDataset> job = (SQLEngineJob<SQLDataset>) jobs.get(execJobKey);
      waitForJobAndHandleExceptionInternal(job);
      return job.waitFor();
    } else {
      throw new IllegalArgumentException("No SQL Engine job exists for stage " + stageName);
    }
  }

  /**
   * Join implementation. This method has blocking calls and should be executed in a separate thread.
   *
   * @param joinRequest the Join Request
   * @throws SQLEngineException   if any of the preceding jobs fails.
   * @return
   */
  private SQLDataset joinInternal(SQLJoinRequest joinRequest)
    throws SQLEngineException {

    String datasetName = joinRequest.getDatasetName();
    DefaultStageMetrics stageMetrics = new DefaultStageMetrics(metrics, datasetName);
    StageStatisticsCollector statisticsCollector = statsCollectors.get(datasetName);

    // Count input metrics for each of the preceding stages.
    for (SQLDataset inputDataset : joinRequest.getInputDatasets()) {
      countRecordsIn(inputDataset, statisticsCollector, stageMetrics);
    }

    //Execute Join job.
    SQLDataset joinDataset = (SQLDataset) sqlEngine.join(joinRequest);

    // Count output rows and complete future.
    countRecordsOut(joinDataset, statisticsCollector, stageMetrics);
    return joinDataset;
  }

  /**
   * Stops all jobs from executing and cleans up the SQL engine.
   */
  @Override
  public void close() throws RuntimeException {
    RuntimeException ex = null;

    Set<String> datasetNames = new HashSet<>();
    // Stop all jobs
    for (SQLEngineJob<?> job : jobs.values()) {
      try {
        datasetNames.add(job.getDatasetName());
        job.cancel();
      } catch (Throwable t) {
        if (ex == null) {
          ex = new RuntimeException(t);
        } else {
          ex.addSuppressed(t);
        }
      }
    }
    // Cleanup all datasets from the SQL engine.
    for (String datasetName : datasetNames) {
      try {
        sqlEngine.cleanup(datasetName);
      } catch (SQLEngineException t) {
        if (ex == null) {
          ex = t;
        } else {
          ex.addSuppressed(t);
        }
      }
    }
    // Stop the executor service
    executorService.shutdown();

    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Method used to handle execution exceptions from a job.
   *
   * Any error during execution will be wrapped in a SQL exception if needed, and the SQL Engine Adapter will be shut
   * down.
   *
   * @param job the job to wait for.
   * @throws SQLEngineException if the internal task threw an exception.
   */
  private void waitForJobAndHandleExceptionInternal(SQLEngineJob<?> job) throws SQLEngineException {
    SQLEngineException ex = null;

    try {
      job.waitFor();
    } catch (CompletionException ce) {
      LOG.error("SQL Engine Task completed exceptionally");
      try {
        throw ce.getCause();
      } catch (SQLEngineException see) {
        // If the source of this was an SQL exception, just rethrow.
        LOG.error("SQL Engine Task failed with exception.", see);
        ex = see;
      } catch (Throwable t) {
        // Wrap any other exception in SQL exception
        LOG.error("SQL Engine Task failed with exception.", t);
        ex = new SQLEngineException(t);
      }
    } catch (CancellationException ce) {
      LOG.error("SQL Engine Task was cancelled", ce);
      ex = new SQLEngineException(ce);
    } catch (Throwable t) {
      LOG.error("SQL Engine Task failed with unexpected throwable", t);
      ex = new SQLEngineException(t);
    }
    // Throw SQL Exception if needed.
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Block until an SQL Engine job is completed.
   *
   * If an exception is thrown by this job, this stops the SQL engine and executed cleanup operations.
   *
   * @param job the job to wait for completion
   * @throws SQLEngineException exception thrown when this job was stopped.
   */
  public void waitForJobAndHandleException(SQLEngineJob<?> job) throws SQLEngineException {
    try {
      waitForJobAndHandleExceptionInternal(job);
    } catch (SQLEngineException e) {
      // If an exception is thrown, stop this SQL engine adapter (including all tasks) and cleanup existing datasets
      // from the SQL engine.
      try {
        close();
      } catch (Throwable t) {
        e.addSuppressed(t);
      }
      throw e;
    }
  }

  /**
   * Method to aggregate input metrics.
   *
   * @param dataset                  the dataset to use to count metrics
   * @param stageStatisticsCollector Stage Statistics Collector for this stage
   * @param stageMetrics             Metrics for this stage
   */
  private void countRecordsIn(SQLDataset dataset,
                              @Nullable StageStatisticsCollector stageStatisticsCollector,
                              StageMetrics stageMetrics) {
    // Count input metrics
    if (stageStatisticsCollector != null) {
      stageStatisticsCollector.incrementInputRecordCount(dataset.getNumRows());
    }
    countStageMetrics(stageMetrics, Constants.Metrics.RECORDS_IN, dataset.getNumRows());
  }

  /**
   * Method to aggregate output metrics.
   *
   * @param dataset                  the dataset to use to count metrics
   * @param stageStatisticsCollector Stage Statistics Collector for this stage
   * @param stageMetrics             Metrics for this stage
   */
  private void countRecordsOut(SQLDataset dataset,
                               @Nullable StageStatisticsCollector stageStatisticsCollector,
                               StageMetrics stageMetrics) {
    // Count input metrics
    if (stageStatisticsCollector != null) {
      stageStatisticsCollector.incrementOutputRecordCount(dataset.getNumRows());
    }
    countStageMetrics(stageMetrics, Constants.Metrics.RECORDS_OUT, dataset.getNumRows());
  }

  /**
   * Count records using a Stage Metrics instance for a supplied metric and number of records.
   *
   * Since the Stage Metrics instance only takes integers as counts, this will split the count operation into
   * multiple operations is the number of records to count exceeds INTEGER.MAX_VALUE
   *
   * @param stageMetrics Metrics instance
   * @param metricName   Metric name to count
   * @param numRecords   Number of records to add to the count for the supplied metric.
   */
  protected static void countStageMetrics(StageMetrics stageMetrics,
                                          String metricName,
                                          long numRecords) {
    stageMetrics.countLong(metricName, numRecords);
  }

  /**
   * This method is called when engine is present and is willing to try performing a relational transform.
   * @param stageSpec stage specification
   * @param transform transform plugin
   * @param input input collections
   * @return resulting collection or empty optional if tranform can't be done with this engine
   */
  public Optional<SQLEngineJob<SQLDataset>> tryRelationalTransform(StageSpec stageSpec,
                                                          RelationalTransform transform,
                                                          Map<String, SparkCollection<Object>> input) {
    Map<String, Relation> inputRelations = input.entrySet().stream().collect(Collectors.toMap(
      Map.Entry::getKey,
      e -> sqlEngine.getRelation(new SQLRelationDefinition(e.getKey(), stageSpec.getInputSchemas().get(e.getKey())))
    ));
    BasicRelationalTransformContext pluginContext = new BasicRelationalTransformContext(
      getSQLRelationalEngine(),
      inputRelations);
    if (!transform.transform(pluginContext)) {
      //Plugin was not able to do relational tranform with this engine
      return Optional.empty();
    }
    if (pluginContext.getOutputRelation() == null) {
      //Plugin said that tranformation was success but failed to set output
      throw new IllegalStateException("Plugin " + transform + " did not produce a relational output");
    }
    if (!pluginContext.getOutputRelation().isValid()) {
      //An output is set to invalid relation, probably some of transforms are not supported by an engine
      return Optional.empty();
    }
    //Validate with engine
    SQLTransformDefinition transformDefinition = new SQLTransformDefinition(
      stageSpec.getName(), pluginContext.getOutputRelation(),
      stageSpec.getOutputSchema(),
      Collections.emptyMap(),
      Collections.emptyMap()
    );
    if (!sqlEngine.canTransform(transformDefinition)) {
      return Optional.empty();
    }

    return Optional.of(runJob(stageSpec.getName(), () -> {
      Map<String, SQLDataset> inputDatasets = input.keySet().stream().collect(Collectors.toMap(
        Function.identity(),
        name -> getDatasetForStage(name)
      ));
      SQLTransformRequest sqlContext = new SQLTransformRequest(
        inputDatasets, stageSpec.getName(), pluginContext.getOutputRelation(), stageSpec.getOutputSchema());
      return sqlEngine.transform(sqlContext);
    }));
  }
}
