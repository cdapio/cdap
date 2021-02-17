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
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.spark.CombinedEmitter;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * DatasetAggregationFinalizeFunction finalizes dataset-based aggregation. First it converts into aggregate value
 * any group values were not accumulated during reduce phase. Then it emits results for each group.
 * @param <GROUP_KEY> group key type
 * @param <GROUP_VALUE> type of original value to group
 * @param <AGG_VALUE> type of accumulated values after reduce stage
 * @param <OUT> resulting type
 */
public class DatasetAggregationFinalizeFunction<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT> 
  implements FlatMapFunction<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE>>,
  RecordInfo<Object>> {

  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient BatchReducibleAggregator<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT> aggregator;
  private transient TrackedTransform<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE>>, OUT>
    aggregateTransform;
  private transient CombinedEmitter<OUT> emitter;

  public DatasetAggregationFinalizeFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterator<RecordInfo<Object>> call(
    Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE>> input) throws Exception {
    if (aggregator == null) {
      aggregator = pluginFunctionContext.createAndInitializePlugin(functionCache);
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

  private static class AggregateTransform<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT>
    implements Transformation<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE>>, OUT> {
    private final BatchReducibleAggregator<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT> aggregator;

    AggregateTransform(BatchReducibleAggregator<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT> aggregator) {
      this.aggregator = aggregator;
    }

    @Override
    public void transform(Tuple2<GROUP_KEY, DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE>> input,
                          Emitter<OUT> emitter) throws Exception {

      AGG_VALUE aggValue = input._2().hasAggValue()
        ? input._2().getAggValue()
        : aggregator.initializeAggregateValue(input._2().getGroupValue());
      aggregator.finalize(input._1(), aggValue, emitter);
    }
  }
}
