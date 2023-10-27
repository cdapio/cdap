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

import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineOutput;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.engine.SQLEngineJob;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Spark Collection representing records stored in a SQL engine.
 * <p>
 * Records will be pulled into Spark and operations delegated to an RDDCollection as needed.
 *
 * @param <T> type of records stored in this {@link SparkCollection}.
 */
public class SQLEngineCollection<T> implements SQLBackedCollection<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SQLEngineCollection.class);
  private static final String DIRECT_WRITE_ERROR = "Exception when trying to write to sink {} using direct output. "
    + "Operation will continue with standard sink flow.";
  private final JavaSparkExecutionContext sec;
  private final JavaSparkContext jsc;
  private final SQLContext sqlContext;
  private final DatasetContext datasetContext;
  private final SparkBatchSinkFactory sinkFactory;
  private final FunctionCache.Factory functionCacheFactory;
  private final String datasetName;
  private final BatchSQLEngineAdapter adapter;
  private final SQLEngineJob<SQLDataset> job;
  private BatchCollection<T> localCollection;

  public SQLEngineCollection(JavaSparkExecutionContext sec,
                             FunctionCache.Factory functionCacheFactory,
                             JavaSparkContext jsc,
                             SQLContext sqlContext,
                             DatasetContext datasetContext,
                             SparkBatchSinkFactory sinkFactory,
                             BatchCollection<T> localCollection,
                             String datasetName,
                             BatchSQLEngineAdapter adapter,
                             SQLEngineJob<SQLDataset> job) {
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.functionCacheFactory = functionCacheFactory;
    this.datasetName = datasetName;
    this.adapter = adapter;
    this.job = job;
    this.localCollection = localCollection;
  }

  public SQLEngineCollection(JavaSparkExecutionContext sec,
                             FunctionCache.Factory functionCacheFactory,
                             JavaSparkContext jsc,
                             SQLContext sqlContext,
                             DatasetContext datasetContext,
                             SparkBatchSinkFactory sinkFactory,
                             String datasetName,
                             BatchSQLEngineAdapter adapter,
                             SQLEngineJob<SQLDataset> job) {
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.functionCacheFactory = functionCacheFactory;
    this.datasetName = datasetName;
    this.adapter = adapter;
    this.job = job;
    this.localCollection = null;
  }

  /**
   * If an operation needs to be executed outside of the scope of a SQL Engine, we will need to pull this SQL collection
   * into an RDDCollection and delegate the operation to the RDD collection.
   *
   * @return (@ link RDDCollection } representing the records pulled from the SQL Engine.
   */
  @SuppressWarnings("raw")
  protected BatchCollection<T> pull() {
    // Ensure the local collection is only generated once across multiple threads
    synchronized (this) {
      if (localCollection == null) {
        SQLEngineJob<BatchCollectionFactory<T>> pullJob = adapter.pull(job);
        adapter.waitForJobAndHandleException(pullJob);
        BatchCollectionFactory<T> pullResult = pullJob.waitFor();
        localCollection = pullResult.create(sec, jsc, sqlContext, datasetContext,
            sinkFactory, functionCacheFactory);
      }
    }
    return localCollection;
  }

  @Override
  public <C> C getUnderlying() {
    return (C) pull().getUnderlying();
  }

  @Override
  public DataframeCollection toDataframeCollection(Schema schema) {
    return pull().toDataframeCollection(schema);
  }

  @Override
  public SparkCollection<T> cache() {
    return pull().cache();
  }

  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return pull().union(other);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> transform(StageSpec stageSpec, StageStatisticsCollector collector) {
    return pull().transform(stageSpec, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    return pull().transform(stageSpec, collector);
  }

  @Override
  public <U> SparkCollection<U> map(Function<T, U> function) {
    return pull().map(function);
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return pull().flatMap(stageSpec, function);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec,
                                                       @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    return pull().aggregate(stageSpec, partitions, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec,
                                                             @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    return pull().reduceAggregate(stageSpec, partitions, collector);
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return pull().flatMapToPair(function);
  }

  @Override
  public <U> SparkCollection<U> compute(StageSpec stageSpec, SparkCompute<T, U> compute) throws Exception {
    return pull().compute(stageSpec, compute);
  }

  @Override
  public boolean tryStoreDirect(StageSpec stageSpec) {
    String stageName = stageSpec.getName();

    // Check if this stage should be excluded from executing in the SQL engine
    if (adapter.getExcludedStageNames().contains(stageName)) {
      return false;
    }

    // Get SQLEngineOutput instance for this stage
    SQLEngineOutput sqlEngineOutput = sinkFactory.getSQLEngineOutput(stageName);
    if (sqlEngineOutput != null) {
      // Try writing directly.
      // Exceptions are handled and logged so standard sink flow takes over in case of failure.
      try {
        SQLEngineJob<Boolean> writeJob = adapter.write(datasetName, sqlEngineOutput);
        adapter.waitForJobAndThrowException(writeJob);
        return writeJob.waitFor();
      } catch (SQLEngineException e) {
        LOG.warn(DIRECT_WRITE_ERROR, stageName, e);
      }
    }
    return false;
  }

  @Override
  public Set<String> tryMultiStoreDirect(PhaseSpec phaseSpec, Set<String> sinks) {
    // Set to store names of all consumed sinks.
    Set<String> directStoreSinks = new HashSet<>();

    // Create list to store all tasks.
    List<Future<String>> directStoreFutures = new ArrayList<>(sinks.size());

    // Try to run the direct store task on all sink stages.
    for (String sinkName : sinks) {
      StageSpec stageSpec = phaseSpec.getPhase().getStage(sinkName);

      // Check if we are able to write this output directly
      if (stageSpec != null) {
        // Create an async task that is used to wait for the direct store task to complete.
        Supplier<String> task = () -> {
          // If the direct store task succeeds, we return the sink name. Otherwise, return null.
          if (tryStoreDirect(stageSpec)) {
            return sinkName;
          }
          return null;
        };
        // We submit these in parallel to prevent blocking for each store task to complete in sequence.
        directStoreFutures.add(adapter.submitTask(task));
      }
    }

    // Wait for all the direct store tasks for this group, if any.
    for (Future<String> supplier : directStoreFutures) {
      try {
        // Get sink name from supplier
        String sinkName = supplier.get();

        // If the sink name is not null, it means this stage was consumed successfully.
        if (sinkName != null) {
          directStoreSinks.add(sinkName);
        }
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      } catch (ExecutionException e) {
        // We don't propagate this exception as the regular sink workflow can continue.
        LOG.warn("Execution exception when executing Direct store task. Sink will proceed with default output.", e);
      }
    }

    return directStoreSinks;
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, PairFlatMapFunction<T, Object, Object> sinkFunction) {
    return () -> pull().createStoreTask(stageSpec, sinkFunction).run();
  }

  @Override
  public Runnable createMultiStoreTask(PhaseSpec phaseSpec, Set<String> group, Set<String> sinks,
                                       Map<String, StageStatisticsCollector> collectors) {
    return () -> pull().createMultiStoreTask(phaseSpec, group, sinks, collectors).run();
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, SparkSink<T> sink) throws Exception {
    return () -> {
      try {
        pull().createStoreTask(stageSpec, sink).run();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    };
  }

  @Override
  public void publishAlerts(StageSpec stageSpec, StageStatisticsCollector collector) throws Exception {
    pull().publishAlerts(stageSpec, collector);
  }

  @Override
  public SparkCollection<T> window(StageSpec stageSpec, Windower windower) {
    return pull().window(stageSpec, windower);
  }

  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    String joinStageName = joinRequest.getStageName();
    SQLEngineJob<SQLDataset> job = adapter.join(joinStageName, joinRequest.getJoinDefinition());
    return new SQLEngineCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory,
                                     joinStageName, adapter, job);
  }

  @Override
  public SparkCollection<T> join(JoinExpressionRequest joinRequest) {
    String joinStageName = joinRequest.getStageName();
    SQLEngineJob<SQLDataset> job = adapter.join(joinStageName, joinRequest.getJoinDefinition());
    return new SQLEngineCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory,
                                     joinStageName, adapter, job);
  }
}
