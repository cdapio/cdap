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

import io.cdap.cdap.etl.api.ErrorRecord;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.spark.CombinedEmitter;

/**
 * Function that uses an ErrorTransform to perform a flatmap.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <T> type of input object
 * @param <U> type of output object
 */
public class ErrorTransformFunction<T, U> implements FlatMapFunc<ErrorRecord<T>, RecordInfo<Object>> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient TrackedTransform<ErrorRecord<T>, U> transform;
  private transient CombinedEmitter<U> emitter;

  public ErrorTransformFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterable<RecordInfo<Object>> call(ErrorRecord<T> inputError) throws Exception {
    if (transform == null) {
      ErrorTransform<T, U> plugin = pluginFunctionContext.createAndInitializePlugin(functionCache);
      transform = new TrackedTransform<>(plugin, pluginFunctionContext.createStageMetrics(),
                                         pluginFunctionContext.getDataTracer(),
                                         pluginFunctionContext.getStageStatisticsCollector());
      emitter = new CombinedEmitter<>(pluginFunctionContext.getStageName());
    }
    emitter.reset();
    transform.transform(inputError, emitter);
    return emitter.getEmitted();
  }
}
