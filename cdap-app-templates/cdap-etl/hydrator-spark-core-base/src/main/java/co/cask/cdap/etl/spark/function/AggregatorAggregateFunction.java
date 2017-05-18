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

package co.cask.cdap.etl.spark.function;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.spark.CombinedEmitter;
import scala.Tuple2;

/**
 * Function that uses a BatchAggregator to perform the aggregate part of the aggregator.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <GROUP_KEY> type of group key
 * @param <GROUP_VAL> type of group value
 * @param <OUT> type of aggregate output
 */
public class AggregatorAggregateFunction<GROUP_KEY, GROUP_VAL, OUT>
  implements FlatMapFunc<Tuple2<GROUP_KEY, Iterable<GROUP_VAL>>, Tuple2<Boolean, Object>> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<Tuple2<GROUP_KEY, Iterable<GROUP_VAL>>, OUT> aggregateTransform;
  private transient CombinedEmitter<OUT> emitter;

  public AggregatorAggregateFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Tuple2<Boolean, Object>> call(Tuple2<GROUP_KEY, Iterable<GROUP_VAL>> input) throws Exception {
    if (aggregateTransform == null) {
      BatchAggregator<GROUP_KEY, GROUP_VAL, OUT> aggregator = pluginFunctionContext.createPlugin();
      aggregator.initialize(pluginFunctionContext.createBatchRuntimeContext());
      aggregateTransform = new TrackedTransform<>(new AggregateTransform<>(aggregator),
                                                  pluginFunctionContext.createStageMetrics(),
                                                  "aggregator.groups",
                                                  TrackedTransform.RECORDS_OUT, pluginFunctionContext.getDataTracer());
      emitter = new CombinedEmitter<>(pluginFunctionContext.getStageName());
    }
    emitter.reset();
    aggregateTransform.transform(input, emitter);
    return emitter.getEmitted();
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
