/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.streaming;

import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.batch.BatchSinkRunnableProvider;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.etl.spark.streaming.function.SerializableCallable;
import io.cdap.cdap.etl.spark.streaming.function.StreamingBatchSinkFunction;
import io.cdap.cdap.etl.spark.streaming.function.StreamingMultiSinkFunction;
import io.cdap.cdap.etl.spark.streaming.function.StreamingSparkSinkFunction;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.spark.streaming.Time;

/**
 * Runnable provider for sinks in streaming pipelines.
 */
public class StreamingSinkRunnableProvider extends BatchSinkRunnableProvider {

  private final boolean stateStoreEnabled;
  private final SerializableCallable batchRetryFunction;
  private final long batchTime;
  private final JavaSparkExecutionContext sec;
  private final StreamingRetrySettings streamingRetrySettings;

  public StreamingSinkRunnableProvider(JavaSparkExecutionContext sec, boolean stateStoreEnabled,
      StreamingRetrySettings streamingRetrySettings, SerializableCallable batchRetryFunction,
      long batchTime) {

    this.stateStoreEnabled = stateStoreEnabled;
    this.batchRetryFunction = batchRetryFunction;
    this.batchTime = batchTime;
    this.sec = sec;
    this.streamingRetrySettings = streamingRetrySettings;
  }

  @Override
  public Runnable getBatchSinkRunnable(StageSpec stageSpec, SparkCollection<Object> stageData,
      FunctionCache.Factory functionCacheFactory, PluginFunctionContext pluginFunctionContext) {
    if (!stateStoreEnabled) {
      return super.getBatchSinkRunnable(stageSpec, stageData, functionCacheFactory,
          pluginFunctionContext);
    }

    return () -> {
      try {
        new StreamingBatchSinkFunction<>(sec, stageSpec, functionCacheFactory.newCache(),
            streamingRetrySettings, batchRetryFunction)
            .call(stageData.getUnderlying(), Time.apply(batchTime));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public Runnable getSparkSinkRunnable(StageSpec stageSpec, SparkCollection<Object> stageData,
      SparkSink<Object> sparkSink) throws Exception {
    if (!stateStoreEnabled) {
      return super.getSparkSinkRunnable(stageSpec, stageData, sparkSink);
    }

    return () -> {
      try {
        new StreamingSparkSinkFunction<>(sec, stageSpec, streamingRetrySettings, batchRetryFunction)
            .call(stageData.getUnderlying(), Time.apply(batchTime));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public Runnable getGroupSinkRunnable(PhaseSpec phaseSpec,
      SparkCollection<RecordInfo<Object>> stageData, Set<String> groupStages,
      Map<String, StageStatisticsCollector> collectors, Set<String> groupSinks) {
    if (!stateStoreEnabled) {
      return super.getGroupSinkRunnable(phaseSpec, stageData, groupStages, collectors, groupSinks);
    }

    return () -> {
      try {
        new StreamingMultiSinkFunction(sec, phaseSpec, groupStages, groupSinks, collectors,
            streamingRetrySettings, batchRetryFunction)
            .call(stageData.getUnderlying(), Time.apply(batchTime));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
}
