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

package io.cdap.cdap.etl.spark.function;

import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import org.apache.spark.api.java.function.ReduceFunction;

/**
 * Central part of aggregation that reduces pairs of accumulators. Each accumulator on input may contain either
 * original group value or accumulated value from other reduce. This function reduces two value using
 * mergeValues or mergePartitions and emits a single accumulator with accumulated value.
 * @param <GROUP_VALUE> type of original value to group
 * @param <AGG_VALUE> type of accumulated values after reduce stage
 */
public class DatasetAggregationReduceFunction<GROUP_VALUE, AGG_VALUE>
  implements ReduceFunction<DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE>> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient BatchReducibleAggregator<?, GROUP_VALUE, AGG_VALUE, ?> aggregator;

  public DatasetAggregationReduceFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE> call(
    DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE> a1,
    DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE> a2) throws Exception {
    if (aggregator == null) {
      aggregator = pluginFunctionContext.createAndInitializePlugin(functionCache);
    }
    if (a1.hasAggValue() && a2.hasAggValue()) {
      return DatasetAggregationAccumulator.fromAggValue(aggregator.mergePartitions(a1.getAggValue(), a2.getAggValue()));
    }
    if (a1.hasAggValue()) {
      return DatasetAggregationAccumulator.fromAggValue(aggregator.mergeValues(a1.getAggValue(), a2.getGroupValue()));
    }
    if (a2.hasAggValue()) {
      return DatasetAggregationAccumulator.fromAggValue(aggregator.mergeValues(a2.getAggValue(), a1.getGroupValue()));
    }
    return DatasetAggregationAccumulator.fromAggValue(aggregator.mergeValues(
      aggregator.initializeAggregateValue(a1.getGroupValue()),
      a2.getGroupValue()));
  }
}
