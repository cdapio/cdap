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

import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultEmitter;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.common.plugin.JoinerBridge;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

/**
 * Function that merges a join result using a BatchJoiner.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <JOIN_KEY> the type of join key
 * @param <INPUT_RECORD> the type of input record
 * @param <OUT> the type of output object
 */
public class JoinMergeFunction<JOIN_KEY, INPUT_RECORD, OUT>
  implements FlatMapFunction<Tuple2<JOIN_KEY, List<JoinElement<INPUT_RECORD>>>, OUT> {
  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient TrackedTransform<Tuple2<JOIN_KEY, List<JoinElement<INPUT_RECORD>>>, OUT> joinFunction;
  private transient DefaultEmitter<OUT> emitter;

  public JoinMergeFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public Iterator<OUT> call(Tuple2<JOIN_KEY, List<JoinElement<INPUT_RECORD>>> input) throws Exception {
    if (joinFunction == null) {
      BatchJoiner<JOIN_KEY, INPUT_RECORD, OUT> joiner = functionCache.getValue(this::createInitializedJoiner);
      joinFunction = new TrackedTransform<>(new JoinOnTransform<>(joiner),
                                            pluginFunctionContext.createStageMetrics(),
                                            Constants.Metrics.JOIN_KEYS,
                                            Constants.Metrics.RECORDS_OUT, pluginFunctionContext.getDataTracer(),
                                            pluginFunctionContext.getStageStatisticsCollector());
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    joinFunction.transform(input, emitter);
    return emitter.getEntries().iterator();
  }

  private <K, V, O> BatchJoiner<K, V, O> createInitializedJoiner() throws Exception {
    Object plugin = pluginFunctionContext.createPlugin();
    BatchJoiner<K, V, O> joiner;
    if (plugin instanceof BatchAutoJoiner) {
      String stageName = pluginFunctionContext.getStageName();
      BatchAutoJoiner autoJoiner = (BatchAutoJoiner) plugin;
      AutoJoinerContext autoJoinerContext = pluginFunctionContext.createAutoJoinerContext();
      JoinDefinition joinDefinition = autoJoiner.define(autoJoinerContext);
      autoJoinerContext.getFailureCollector().getOrThrowException();
      if (joinDefinition == null) {
        throw new IllegalStateException(String.format(
          "Join stage '%s' did not specify a join definition. " +
            "Check with the plugin developer to ensure it is implemented correctly.", stageName));
      }
      joiner = new JoinerBridge(stageName, autoJoiner, joinDefinition);
    } else {
      joiner = (BatchJoiner<K, V, O>) plugin;
      BatchJoinerRuntimeContext context = pluginFunctionContext.createBatchRuntimeContext();
      joiner.initialize(context);
    }
    return joiner;
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
