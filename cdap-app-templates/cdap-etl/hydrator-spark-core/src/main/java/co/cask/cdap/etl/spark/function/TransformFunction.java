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

import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.TrackedTransform;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Function that uses a Transform to perform a flatmap.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <T> type of input object
 * @param <U> type of output object
 */
public class TransformFunction<T, U> implements FlatMapFunction<T, U> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<T, U> transform;
  private transient DefaultEmitter<U> emitter;

  public TransformFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<U> call(T input) throws Exception {
    if (transform == null) {
      Transform<T, U> plugin = pluginFunctionContext.createPlugin();
      plugin.initialize(pluginFunctionContext.createBatchRuntimeContext());
      transform = new TrackedTransform<>(plugin, pluginFunctionContext.createStageMetrics(),
                                         pluginFunctionContext.getDataTracer());
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    transform.transform(input, emitter);
    return emitter.getEntries();
  }
}
