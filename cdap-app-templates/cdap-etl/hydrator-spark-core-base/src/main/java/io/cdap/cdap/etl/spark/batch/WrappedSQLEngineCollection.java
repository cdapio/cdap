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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.DelegatingSparkCollection;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SQLEngineBackedCollection that wraps another SQLEngineBackedCollection in order to delay the execution of a
 * mapping function.
 *
 * This is currently used to prevent SQL Engine pull operations uniless absolutely needed.
 * @param <T> Type of the wrapped collection records.
 * @param <U> Type of the output collection records.
 */
public class WrappedSQLEngineCollection<T, U> extends DelegatingSparkCollection<U>
    implements SQLBackedCollection<U> {
  private final java.util.function.Function<SparkCollection<T>, SparkCollection<U>> mapper;
  private final SQLBackedCollection<T> wrapped;
  private BatchCollection<U> unwrapped;

  public WrappedSQLEngineCollection(
      SQLBackedCollection<T> wrapped,
      java.util.function.Function<SparkCollection<T>, SparkCollection<U>> mapper) {
    this.wrapped = wrapped;
    this.mapper = mapper;
  }

  protected BatchCollection<U> getDelegate() {
    if (unwrapped == null) {
      unwrapped = (BatchCollection<U>) mapper.apply(wrapped);
    }

    return unwrapped;
  }

  @Override
  public DataframeCollection toDataframeCollection(Schema schema) {
    return getDelegate().toDataframeCollection(schema);
  }

  /**
   * Executes an operation on the underlying collection and then wraps it in the same mapper for this collection.
   *
   * This is useful when executing multiple operations in sequence where we need to delegate an operation to the
   * underlying implementation
   *
   * By calling this over all wrapped collections, we will eventually reach an instance of a
   * {@link SQLEngineCollection} where the actual operation will take place.
   *
   * @param remapper function used to re-map the underlying collection.
   * @return SQL Backed collection after re-mapping the underlying colleciton and re-adding the mapper.
   */
  protected SparkCollection<U> rewrap(
    java.util.function.Function<SparkCollection<T>, SparkCollection<T>> remapper) {
    return new WrappedSQLEngineCollection<>((SQLBackedCollection<T>) remapper.apply(wrapped), mapper);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec,
                                  PairFlatMapFunction<U, Object, Object> sinkFunction) {
    return new Runnable() {
      @Override
      public void run() {
        // Run direct store job. If this succeeds, complete execution.
        if (wrapped.tryStoreDirect(stageSpec)) {
          return;
        }

        // Run store task on the unwrapped collection if the direct store task could not be completed.
        getDelegate().createStoreTask(stageSpec, sinkFunction).run();
      }
    };
  }

  @Override
  public boolean tryStoreDirect(StageSpec stageSpec) {
    return wrapped.tryStoreDirect(stageSpec);
  }

  @Override
  public Set<String> tryMultiStoreDirect(PhaseSpec phaseSpec, Set<String> sinks) {
    return wrapped.tryMultiStoreDirect(phaseSpec, sinks);
  }

  /**
   * Handle logic to store directly to the Sink stage if the sink supports this behavior.
   *
   * @param stageSpec stage spec
   * @param sinkFunction sync function to use
   * @return Runnable that can be used to execute this store task.
   */

  /**
   * Handle logic to store directly to the multiple sinks if this behavior is supported by the sinks.
   *
   * @param phaseSpec phase spec
   * @param group set containing all stages in the group
   * @param sinks set containing all sinks in the group
   * @param collectors map containing all stats collectors
   * @return runnable that can be used to execute this multi store operation
   */
  @Override
  public Runnable createMultiStoreTask(PhaseSpec phaseSpec,
                                       Set<String> group,
                                       Set<String> sinks,
                                       Map<String, StageStatisticsCollector> collectors) {
    return new Runnable() {
      @Override
      public void run() {
        // Copy all sinks and groups into a ConcurrentHashSet set
        Set<String> remainingGroups = new HashSet<>(group);
        Set<String> remainingSinks = new HashSet<>(sinks);

        // Invoke multi store direct task on wrapped collection
        Set<String> consumedSinks = wrapped.tryMultiStoreDirect(phaseSpec, sinks);

        // Removed all consumed sinks by the Multi store task from this group.
        remainingGroups.removeAll(consumedSinks);
        remainingSinks.removeAll(consumedSinks);

        // If all sinks were consumed, there is no more output to write.
        if (remainingSinks.isEmpty()) {
          return;
        }

        // Delegate all remaining sinks and group stages to the original multi store task.
        getDelegate().createMultiStoreTask(phaseSpec, remainingGroups, remainingSinks, collectors).run();
      }
    };
  }

  @Override
  public SparkCollection<U> join(JoinRequest joinRequest) {
    return rewrap(c -> c.join(joinRequest));
  }

  @Override
  public SparkCollection<U> join(JoinExpressionRequest joinExpressionRequest) {
    return rewrap(c -> c.join(joinExpressionRequest));
  }
}
