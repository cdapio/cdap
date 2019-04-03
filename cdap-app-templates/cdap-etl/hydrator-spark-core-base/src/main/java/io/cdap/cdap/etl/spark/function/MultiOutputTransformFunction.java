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

package co.cask.cdap.etl.spark.function;

import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.TrackedMultiOutputTransform;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.spark.CombinedEmitter;

/**
 * Function that uses a MultiOutputTransform to perform a flatmap.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <T> type of input object
 */
public class MultiOutputTransformFunction<T> implements FlatMapFunc<T, RecordInfo<Object>> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedMultiOutputTransform<T, Object> transform;
  private transient CombinedEmitter<Object> emitter;

  public MultiOutputTransformFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<RecordInfo<Object>> call(T input) throws Exception {
    if (transform == null) {
      SplitterTransform<T, Object> plugin = pluginFunctionContext.createPlugin();
      plugin.initialize(pluginFunctionContext.createBatchRuntimeContext());
      transform = new TrackedMultiOutputTransform<>(plugin, pluginFunctionContext.createStageMetrics(),
                                                    pluginFunctionContext.getDataTracer());
      emitter = new CombinedEmitter<>(pluginFunctionContext.getStageName());
    }
    emitter.reset();
    transform.transform(input, emitter);
    return emitter.getEmitted();
  }
}
