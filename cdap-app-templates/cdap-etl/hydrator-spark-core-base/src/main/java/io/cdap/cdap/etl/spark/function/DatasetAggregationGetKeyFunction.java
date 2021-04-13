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

import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultEmitter;
import io.cdap.cdap.etl.common.NoErrorEmitter;
import io.cdap.cdap.etl.common.TrackedTransform;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * This function is used as a first step in dataset-based aggregation. It retrieves group key(s) and emits
 * tuples with group key and an accumulator with group value in each tuple.
 * @param <GROUP_KEY> group key type
 * @param <GROUP_VAL> group value type
 * @param <AGG_VALUE> type to accumulate aggregated value
 */
public class DatasetAggregationGetKeyFunction<GROUP_KEY, GROUP_VAL, AGG_VALUE>
  implements FlatMapFunction<GROUP_VAL, Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VAL, AGG_VALUE>>> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient TrackedTransform<GROUP_VAL,
    Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VAL, AGG_VALUE>>> groupByFunction;
  private transient DefaultEmitter<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VAL, AGG_VALUE>>> emitter;


  public DatasetAggregationGetKeyFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterator<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VAL, AGG_VALUE>>> call(GROUP_VAL input)
    throws Exception {
    if (groupByFunction == null) {
      BatchReducibleAggregator<GROUP_KEY, GROUP_VAL, ?, ?> aggregator =
        pluginFunctionContext.createAndInitializePlugin(functionCache);
      groupByFunction = new TrackedTransform<>(new GroupByTransform<>(aggregator),
                                               pluginFunctionContext.createStageMetrics(),
                                               Constants.Metrics.RECORDS_IN,
                                               null, pluginFunctionContext.getDataTracer(),
                                               pluginFunctionContext.getStageStatisticsCollector());
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    groupByFunction.transform(input, emitter);
    return emitter.getEntries().iterator();
  }

  private static class GroupByTransform<GROUP_KEY, GROUP_VAL, AGG_VALUE>
    implements Transformation<GROUP_VAL, Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VAL, AGG_VALUE>>> {
    private final BatchReducibleAggregator<GROUP_KEY, GROUP_VAL, ?, ?> aggregator;
    private final NoErrorEmitter<GROUP_KEY> keyEmitter;

    GroupByTransform(BatchReducibleAggregator<GROUP_KEY, GROUP_VAL, ?, ?> aggregator) {
      this.aggregator = aggregator;
      this.keyEmitter =
        new NoErrorEmitter<>("Errors and Alerts cannot be emitted from the groupBy method of an aggregator");
    }

    @Override
    public void transform(final GROUP_VAL inputValue,
                          Emitter<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VAL, AGG_VALUE>>> emitter)
      throws Exception {
      keyEmitter.reset();
      aggregator.groupBy(inputValue, keyEmitter);
      for (GROUP_KEY key : keyEmitter.getEntries()) {
        emitter.emit(new Tuple2<>(key, DatasetAggregationAccumulator.fromGroupValue(inputValue)));
      }
    }
  }
}
