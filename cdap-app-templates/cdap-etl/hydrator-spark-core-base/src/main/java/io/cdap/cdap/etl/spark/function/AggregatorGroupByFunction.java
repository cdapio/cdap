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

package io.cdap.cdap.etl.spark.function;

import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultEmitter;
import io.cdap.cdap.etl.common.NoErrorEmitter;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.common.plugin.AggregatorBridge;
import scala.Tuple2;

/**
 * Function that uses a BatchAggregator or a AggregatorBridge depending on the type of the aggregator
 * to perform the groupBy part of the aggregator.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <GROUP_KEY> type of group key
 * @param <GROUP_VAL> type of group val
 */
public class AggregatorGroupByFunction<GROUP_KEY, GROUP_VAL>
  implements PairFlatMapFunc<GROUP_VAL, GROUP_KEY, GROUP_VAL> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient TrackedTransform<GROUP_VAL, Tuple2<GROUP_KEY, GROUP_VAL>> groupByFunction;
  private transient DefaultEmitter<Tuple2<GROUP_KEY, GROUP_VAL>> emitter;

  public AggregatorGroupByFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterable<Tuple2<GROUP_KEY, GROUP_VAL>> call(GROUP_VAL input) throws Exception {
    if (groupByFunction == null) {
      Object plugin = pluginFunctionContext.createAndInitializePlugin(functionCache);
      BatchAggregator<GROUP_KEY, GROUP_VAL, ?> aggregator;
      if (plugin instanceof BatchReducibleAggregator) {
        BatchReducibleAggregator<GROUP_KEY, GROUP_VAL, ?, ?> reducibleAggregator =
          (BatchReducibleAggregator<GROUP_KEY, GROUP_VAL, ?, ?>) plugin;
        aggregator = new AggregatorBridge<>(reducibleAggregator);
      } else {
        aggregator = (BatchAggregator<GROUP_KEY, GROUP_VAL, ?>) plugin;
      }
      groupByFunction = new TrackedTransform<>(new GroupByTransform<>(aggregator),
                                               pluginFunctionContext.createStageMetrics(),
                                               Constants.Metrics.RECORDS_IN,
                                               null, pluginFunctionContext.getDataTracer(),
                                               pluginFunctionContext.getStageStatisticsCollector());
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    groupByFunction.transform(input, emitter);
    return emitter.getEntries();
  }

  private static class GroupByTransform<GROUP_KEY, GROUP_VAL>
    implements Transformation<GROUP_VAL, Tuple2<GROUP_KEY, GROUP_VAL>> {
    private final BatchAggregator<GROUP_KEY, GROUP_VAL, ?> aggregator;
    private final NoErrorEmitter<GROUP_KEY> keyEmitter;

    GroupByTransform(BatchAggregator<GROUP_KEY, GROUP_VAL, ?> aggregator) {
      this.aggregator = aggregator;
      this.keyEmitter =
        new NoErrorEmitter<>("Errors and Alerts cannot be emitted from the groupBy method of an aggregator");
    }

    @Override
    public void transform(final GROUP_VAL inputValue, Emitter<Tuple2<GROUP_KEY, GROUP_VAL>> emitter) throws Exception {
      keyEmitter.reset();
      aggregator.groupBy(inputValue, keyEmitter);
      for (GROUP_KEY key : keyEmitter.getEntries()) {
        emitter.emit(new Tuple2<>(key, inputValue));
      }
    }
  }
}
