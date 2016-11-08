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
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.TrackedTransform;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * Function that uses a BatchJoiner to perform the joinOn part of the join.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <JOIN_KEY> the type of join key
 * @param <INPUT_RECORD> the type of input records to the join stage
 */
public class JoinOnFunction<JOIN_KEY, INPUT_RECORD>
  implements PairFlatMapFunction<INPUT_RECORD, JOIN_KEY, INPUT_RECORD> {

  private final PluginFunctionContext pluginFunctionContext;
  private final String inputStageName;
  private transient TrackedTransform<INPUT_RECORD, Tuple2<JOIN_KEY, INPUT_RECORD>> joinFunction;
  private transient DefaultEmitter<Tuple2<JOIN_KEY, INPUT_RECORD>> emitter;

  public JoinOnFunction(PluginFunctionContext pluginFunctionContext, String inputStageName) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.inputStageName = inputStageName;
  }

  @Override
  public Iterable<Tuple2<JOIN_KEY, INPUT_RECORD>> call(INPUT_RECORD input) throws Exception {
    if (joinFunction == null) {
      BatchJoiner<JOIN_KEY, INPUT_RECORD, Object> joiner = pluginFunctionContext.createPlugin();
      BatchJoinerRuntimeContext context = pluginFunctionContext.createJoinerRuntimeContext();
      joiner.initialize(context);
      joinFunction = new TrackedTransform<>(new JoinOnTransform<>(joiner, inputStageName),
                                               pluginFunctionContext.createStageMetrics(),
                                               TrackedTransform.RECORDS_IN,
                                               null);
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    joinFunction.transform(input, emitter);
    return emitter.getEntries();
  }

  private static class JoinOnTransform<INPUT, JOIN_KEY> implements Transformation<INPUT, Tuple2<JOIN_KEY, INPUT>> {
    private final BatchJoiner<JOIN_KEY, INPUT, ?> joiner;
    private final String inputStageName;

    JoinOnTransform(BatchJoiner<JOIN_KEY, INPUT, ?> joiner, String inputStageName) {
      this.joiner = joiner;
      this.inputStageName = inputStageName;
    }

    @Override
    public void transform(final INPUT inputValue, Emitter<Tuple2<JOIN_KEY, INPUT>> emitter) throws Exception {
      emitter.emit(new Tuple2<>(joiner.joinOn(inputStageName, inputValue), inputValue));
    }
  }
}
