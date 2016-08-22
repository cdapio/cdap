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

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.TrackedTransform;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Function that merges a join result using a BatchJoiner.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 */
public class JoinMergeFunction implements FlatMapFunction<Tuple2<Object, List<JoinElement<Object>>>, Object> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<Tuple2<Object, List<JoinElement<Object>>>, Object> joinFunction;
  private transient DefaultEmitter<Object> emitter;

  public JoinMergeFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Object> call(Tuple2<Object, List<JoinElement<Object>>> input) throws Exception {
    if (joinFunction == null) {
      BatchJoiner<Object, Object, Object> joiner = pluginFunctionContext.createPlugin();
      BatchJoinerRuntimeContext context = pluginFunctionContext.createJoinerRuntimeContext();
      joiner.initialize(context);
      joinFunction = new TrackedTransform<>(new JoinOnTransform<>(joiner),
                                            pluginFunctionContext.createStageMetrics(),
                                            "joiner.keys",
                                            TrackedTransform.RECORDS_OUT);
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    joinFunction.transform(input, emitter);
    return emitter.getEntries();
  }

  private static class JoinOnTransform<JOIN_KEY, INPUT, OUT>
    implements Transformation<Tuple2<JOIN_KEY, List<JoinElement<INPUT>>>, OUT> {
    private final BatchJoiner<JOIN_KEY, INPUT, OUT> joiner;

    JoinOnTransform(BatchJoiner<JOIN_KEY, INPUT, OUT> joiner) {
      this.joiner = joiner;
    }

    @Override
    public void transform(Tuple2<JOIN_KEY, List<JoinElement<INPUT>>> input, Emitter<OUT> emitter) throws Exception {
      emitter.emit(joiner.merge(input._1(), input._2()));
    }
  }
}
