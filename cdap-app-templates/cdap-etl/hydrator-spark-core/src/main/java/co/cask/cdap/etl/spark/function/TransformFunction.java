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
 * Function that uses a BatchSource to transform a pair of objects into a single object.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 */
public class TransformFunction implements FlatMapFunction<Object, Object> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<Object, Object> transform;
  private transient DefaultEmitter<Object> emitter;

  public TransformFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Object> call(Object input) throws Exception {
    if (transform == null) {
      Transform<Object, Object> batchSource = pluginFunctionContext.createPlugin();
      batchSource.initialize(pluginFunctionContext.createBatchRuntimeContext());
      transform = new TrackedTransform<>(batchSource, pluginFunctionContext.createStageMetrics());
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    transform.transform(input, emitter);
    return emitter.getEntries();
  }
}
