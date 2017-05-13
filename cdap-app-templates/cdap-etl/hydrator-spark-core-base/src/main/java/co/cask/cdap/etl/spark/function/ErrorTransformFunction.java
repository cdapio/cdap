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

import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.spark.CombinedEmitter;
import scala.Tuple2;

/**
 * Function that uses an ErrorTransform to perform a flatmap.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <T> type of input object
 * @param <U> type of output object
 */
public class ErrorTransformFunction<T, U> implements FlatMapFunc<ErrorRecord<T>, Tuple2<Boolean, Object>> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<ErrorRecord<T>, U> transform;
  private transient CombinedEmitter<U> emitter;

  public ErrorTransformFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Tuple2<Boolean, Object>> call(ErrorRecord<T> inputError) throws Exception {
    if (transform == null) {
      ErrorTransform<T, U> plugin = pluginFunctionContext.createPlugin();
      plugin.initialize(pluginFunctionContext.createBatchRuntimeContext());
      transform = new TrackedTransform<>(plugin, pluginFunctionContext.createStageMetrics(),
                                         pluginFunctionContext.getDataTracer());
      emitter = new CombinedEmitter<>(pluginFunctionContext.getStageName());
    }
    emitter.reset();
    transform.transform(inputError, emitter);
    return emitter.getEmitted();
  }
}
