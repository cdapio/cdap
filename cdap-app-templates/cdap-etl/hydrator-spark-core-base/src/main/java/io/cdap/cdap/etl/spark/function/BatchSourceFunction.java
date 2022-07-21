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

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.spark.CombinedEmitter;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Function that uses a BatchSource to transform a pair of objects into a single object.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 */
public class BatchSourceFunction implements FlatMapFunction<Tuple2<Object, Object>, RecordInfo<Object>> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient Transformation<KeyValue<Object, Object>, Object> transform;
  private transient CombinedEmitter<Object> emitter;

  public BatchSourceFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterator<RecordInfo<Object>> call(Tuple2<Object, Object> input) throws Exception {
    if (transform == null) {
      BatchSource<Object, Object, Object> batchSource = pluginFunctionContext.createAndInitializePlugin(functionCache);
      transform = new TrackedTransform<>(batchSource,
                                         pluginFunctionContext.createStageMetrics(),
                                         pluginFunctionContext.getDataTracer(),
                                         pluginFunctionContext.getStageStatisticsCollector());
      emitter = new CombinedEmitter<>(pluginFunctionContext.getStageName());
    }
    emitter.reset();
    KeyValue<Object, Object> inputKV = new KeyValue<>(input._1(), input._2());
    transform.transform(inputKV, emitter);
    return emitter.getEmitted().iterator();
  }
}
