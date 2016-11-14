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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * Function that uses a BatchSink to transform one object into a pair.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 */
public class BatchSinkFunction implements PairFlatMapFunction<Object, Object, Object> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<Object, KeyValue<Object, Object>> transform;
  private transient TransformingEmitter<KeyValue<Object, Object>, Tuple2<Object, Object>> emitter;

  public BatchSinkFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Tuple2<Object, Object>> call(Object input) throws Exception {
    if (transform == null) {
      BatchSink<Object, Object, Object> batchSink = pluginFunctionContext.createPlugin();
      batchSink.initialize(pluginFunctionContext.createBatchRuntimeContext());
      transform = new TrackedTransform<>(batchSink, pluginFunctionContext.createStageMetrics(),
                                         pluginFunctionContext.getDataTracer());
      emitter = new TransformingEmitter<>(new Function<KeyValue<Object, Object>, Tuple2<Object, Object>>() {
        @Override
        public Tuple2<Object, Object> apply(KeyValue<Object, Object> input) {
          return new Tuple2<>(input.getKey(), input.getValue());
        }
      });
    }
    emitter.reset();
    transform.transform(input, emitter);
    return emitter.getEntries();
  }
}
