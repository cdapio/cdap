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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import java.util.Map;
import java.util.Set;

public interface SinkRunnableProvider {

  /**
   * Returns runnable for batch sink.
   *
   * @param stageSpec {@link StageSpec}
   * @param stageData {@link SparkCollection} with stageData.
   * @param functionCacheFactory {@link FunctionCache.Factory}
   * @param pluginFunctionContext {@link PluginFunctionContext}
   * @return {@link Runnable} for the sink.
   */
  Runnable getBatchSinkRunnable(StageSpec stageSpec, SparkCollection<Object> stageData,
      FunctionCache.Factory functionCacheFactory, PluginFunctionContext pluginFunctionContext);

  /**
   * Returns runnable for Spark sink.
   *
   * @param stageSpec {@link StageSpec}
   * @param stageData {@link SparkCollection} with stageData.
   * @param sparkSink {@link SparkSink} reference.
   * @return {@link Runnable} for the sink.
   * @throws Exception Exception thrown by the sink.
   */
  Runnable getSparkSinkRunnable(StageSpec stageSpec, SparkCollection<Object> stageData,
      SparkSink<Object> sparkSink) throws Exception;

  /**
   * Returns runnable for group sink.
   *
   * @param phaseSpec {@link PhaseSpec}
   * @param stageData {@link SparkCollection} with stageData.
   * @param groupStages {@link Set} of stages in the group.
   * @param collectors {@link Map} of stage to {@link StageStatisticsCollector}.
   * @param groupSinks {@link Set} of sinks in the group.
   * @return {@link Runnable} for the sink.
   */
  Runnable getGroupSinkRunnable(PhaseSpec phaseSpec, SparkCollection<RecordInfo<Object>> stageData,
      Set<String> groupStages, Map<String, StageStatisticsCollector> collectors,
      Set<String> groupSinks);
}
