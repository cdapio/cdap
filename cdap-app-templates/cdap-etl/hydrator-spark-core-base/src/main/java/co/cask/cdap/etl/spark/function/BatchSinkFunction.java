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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.common.TransformingEmitter;
import com.google.common.base.Function;
import scala.Tuple2;

/**
 * Function that uses a BatchSink to transform one object into a pair.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <IN> type of input object
 * @param <OUT_KEY> type of output key
 * @param <OUT_VAL> type of output val
 */
public class BatchSinkFunction<IN, OUT_KEY, OUT_VAL> implements PairFlatMapFunc<IN, OUT_KEY, OUT_VAL> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<IN, KeyValue<OUT_KEY, OUT_VAL>> transform;
  private transient TransformingEmitter<KeyValue<OUT_KEY, OUT_VAL>, Tuple2<OUT_KEY, OUT_VAL>> emitter;

  public BatchSinkFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Tuple2<OUT_KEY, OUT_VAL>> call(IN input) throws Exception {
    if (transform == null) {
      BatchSink<IN, OUT_KEY, OUT_VAL> batchSink = pluginFunctionContext.createPlugin();
      batchSink.initialize(pluginFunctionContext.createBatchRuntimeContext());
      transform = new TrackedTransform<>(batchSink, pluginFunctionContext.createStageMetrics(),
                                         pluginFunctionContext.getDataTracer(),
                                         pluginFunctionContext.getStageStatisticsCollector());
      emitter = new TransformingEmitter<>(new Function<KeyValue<OUT_KEY, OUT_VAL>, Tuple2<OUT_KEY, OUT_VAL>>() {
        @Override
        public Tuple2<OUT_KEY, OUT_VAL> apply(KeyValue<OUT_KEY, OUT_VAL> input) {
          return new Tuple2<>(input.getKey(), input.getValue());
        }
      });
    }
    emitter.reset();
    transform.transform(input, emitter);
    return emitter.getEntries();
  }
}
