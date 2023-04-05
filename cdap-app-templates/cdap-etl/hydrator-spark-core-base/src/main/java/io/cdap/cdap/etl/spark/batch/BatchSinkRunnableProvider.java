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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SinkRunnableProvider;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.function.BatchSinkFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import java.util.Map;
import java.util.Set;

/**
 * Runnable provider for sinks in batch pipelines.
 */
public class BatchSinkRunnableProvider implements SinkRunnableProvider {

  @Override
  public Runnable getBatchSinkRunnable(StageSpec stageSpec, SparkCollection<Object> stageData,
      FunctionCache.Factory functionCacheFactory, PluginFunctionContext pluginFunctionContext) {
    return stageData.createStoreTask(stageSpec, new BatchSinkFunction(
        pluginFunctionContext, functionCacheFactory.newCache()));
  }

  @Override
  public Runnable getSparkSinkRunnable(StageSpec stageSpec, SparkCollection<Object> stageData,
      SparkSink<Object> sparkSink) throws Exception {
    return stageData.createStoreTask(stageSpec, sparkSink);
  }

  @Override
  public Runnable getGroupSinkRunnable(PhaseSpec phaseSpec,
      SparkCollection<RecordInfo<Object>> stageData, Set<String> groupStages,
      Map<String, StageStatisticsCollector> collectors, Set<String> groupSinks) {
    return stageData.createMultiStoreTask(phaseSpec, groupStages, groupSinks, collectors);
  }
}
