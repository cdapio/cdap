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
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.common.plugin.AggregatorBridge;
import io.cdap.cdap.etl.spark.CombinedEmitter;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Function that uses a BatchAggregator or a AggregatorBridge depending on the type of the aggregator
 * to perform the aggregate part of the aggregator.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <GROUP_KEY> type of group key
 * @param <GROUP_VAL> type of group value
 * @param <OUT> type of aggregate output
 */
public class AggregatorAggregateFunction<GROUP_KEY, GROUP_VAL, OUT>
  implements FlatMapFunction<Tuple2<GROUP_KEY, Iterable<GROUP_VAL>>, RecordInfo<Object>> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient TrackedTransform<Tuple2<GROUP_KEY, Iterable<GROUP_VAL>>, OUT> aggregateTransform;
  private transient CombinedEmitter<OUT> emitter;

  public AggregatorAggregateFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterator<RecordInfo<Object>> call(Tuple2<GROUP_KEY, Iterable<GROUP_VAL>> input) throws Exception {
    if (aggregateTransform == null) {
      Object plugin = pluginFunctionContext.createAndInitializePlugin(functionCache);
      BatchAggregator<GROUP_KEY, GROUP_VAL, OUT> aggregator;
      if (plugin instanceof BatchReducibleAggregator) {
        BatchReducibleAggregator<GROUP_KEY, GROUP_VAL, ?, OUT> reducibleAggregator =
          (BatchReducibleAggregator<GROUP_KEY, GROUP_VAL, ?, OUT>) plugin;
        aggregator = new AggregatorBridge<>(reducibleAggregator);
      } else {
        aggregator = (BatchAggregator<GROUP_KEY, GROUP_VAL, OUT>) plugin;
      }
      aggregateTransform = new TrackedTransform<>(new AggregateTransform<>(aggregator),
                                                  pluginFunctionContext.createStageMetrics(),
                                                  Constants.Metrics.AGG_GROUPS,
                                                  Constants.Metrics.RECORDS_OUT, pluginFunctionContext.getDataTracer(),
                                                  pluginFunctionContext.getStageStatisticsCollector());
      emitter = new CombinedEmitter<>(pluginFunctionContext.getStageName());
    }
    emitter.reset();
    aggregateTransform.transform(input, emitter);
    return emitter.getEmitted().iterator();
  }

  private static class AggregateTransform<GROUP_KEY, GROUP_VAL, OUT_VAL>
    implements Transformation<Tuple2<GROUP_KEY, Iterable<GROUP_VAL>>, OUT_VAL> {
    private final BatchAggregator<GROUP_KEY, GROUP_VAL, OUT_VAL> aggregator;

    AggregateTransform(BatchAggregator<GROUP_KEY, GROUP_VAL, OUT_VAL> aggregator) {
      this.aggregator = aggregator;
    }

    @Override
    public void transform(Tuple2<GROUP_KEY, Iterable<GROUP_VAL>> input, Emitter<OUT_VAL> emitter) throws Exception {
      aggregator.aggregate(input._1(), input._2().iterator(), emitter);
    }
  }
}
