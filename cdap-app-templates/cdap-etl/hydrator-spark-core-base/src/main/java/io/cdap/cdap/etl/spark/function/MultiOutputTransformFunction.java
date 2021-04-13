/*
 * Copyright © 2017 Cask Data, Inc.
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

import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.TrackedMultiOutputTransform;
import io.cdap.cdap.etl.spark.CombinedEmitter;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

/**
 * Function that uses a MultiOutputTransform to perform a flatmap.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <T> type of input object
 */
public class MultiOutputTransformFunction<T> implements FlatMapFunction<T, RecordInfo<Object>> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient TrackedMultiOutputTransform<T, Object> transform;
  private transient CombinedEmitter<Object> emitter;

  public MultiOutputTransformFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterator<RecordInfo<Object>> call(T input) throws Exception {
    if (transform == null) {
      SplitterTransform<T, Object> plugin = pluginFunctionContext.createAndInitializePlugin(functionCache);
      transform = new TrackedMultiOutputTransform<>(plugin, pluginFunctionContext.createStageMetrics(),
                                                    pluginFunctionContext.getDataTracer());
      emitter = new CombinedEmitter<>(pluginFunctionContext.getStageName());
    }
    emitter.reset();
    transform.transform(input, emitter);
    return emitter.getEmitted().iterator();
  }
}
