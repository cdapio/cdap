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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultEmitter;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.common.plugin.JoinerBridge;
import scala.Tuple2;

/**
 * Function that uses a BatchJoiner to perform the joinOn part of the join.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <JOIN_KEY> the type of join key
 * @param <INPUT_RECORD> the type of input records to the join stage
 */
public class JoinOnFunction<JOIN_KEY, INPUT_RECORD>
  implements PairFlatMapFunc<INPUT_RECORD, JOIN_KEY, INPUT_RECORD> {

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
      Object plugin = pluginFunctionContext.createPlugin();
      BatchJoiner<JOIN_KEY, INPUT_RECORD, Object> joiner;
      boolean filterNullKeys = false;
      if (plugin instanceof BatchAutoJoiner) {
        BatchAutoJoiner autoJoiner = (BatchAutoJoiner) plugin;
        AutoJoinerContext autoJoinerContext = pluginFunctionContext.createAutoJoinerContext();
        JoinDefinition joinDefinition = autoJoiner.define(autoJoinerContext);
        autoJoinerContext.getFailureCollector().getOrThrowException();
        String stageName = pluginFunctionContext.getStageName();
        if (joinDefinition == null) {
          throw new IllegalStateException(String.format(
            "Join stage '%s' did not specify a join definition. " +
              "Check with the plugin developer to ensure it is implemented correctly.", stageName));
        }
        JoinCondition condition = joinDefinition.getCondition();
        /*
           Filter out the record if it comes from an optional stage
           and the key is null, or if any of the fields in the key is null.
           For example, suppose we are performing a left outer join on:

            A (id, name) = (0, alice), (null, bob)
            B (id, email) = (0, alice@example.com), (null, placeholder@example.com)

           The final output should be:

           joined (A.id, A.name, B.email) = (0, alice, alice@example.com), (null, bob, null, null)

           that is, the bob record should not be joined to the placeholder@example email, even though both their
           ids are null.
         */
        if (condition.getOp() == JoinCondition.Op.KEY_EQUALITY && !((JoinCondition.OnKeys) condition).isNullSafe()) {
          filterNullKeys = joinDefinition.getStages().stream()
            .filter(s -> !s.isRequired())
            .map(JoinStage::getStageName)
            .anyMatch(s -> s.equals(inputStageName));
        }
        joiner = new JoinerBridge(stageName, autoJoiner, joinDefinition);
      } else {
        joiner = (BatchJoiner<JOIN_KEY, INPUT_RECORD, Object>) plugin;
        BatchJoinerRuntimeContext context = pluginFunctionContext.createBatchRuntimeContext();
        joiner.initialize(context);
      }

      joinFunction = new TrackedTransform<>(new JoinOnTransform<>(joiner, inputStageName, filterNullKeys),
                                            pluginFunctionContext.createStageMetrics(),
                                            Constants.Metrics.RECORDS_IN,
                                            null, pluginFunctionContext.getDataTracer(),
                                            pluginFunctionContext.getStageStatisticsCollector());
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    joinFunction.transform(input, emitter);
    return emitter.getEntries();
  }

  private static class JoinOnTransform<INPUT, JOIN_KEY> implements Transformation<INPUT, Tuple2<JOIN_KEY, INPUT>> {
    private final BatchJoiner<JOIN_KEY, INPUT, ?> joiner;
    private final String inputStageName;
    private final boolean filterNullKeys;

    JoinOnTransform(BatchJoiner<JOIN_KEY, INPUT, ?> joiner, String inputStageName, boolean filterNullKeys) {
      this.joiner = joiner;
      this.inputStageName = inputStageName;
      this.filterNullKeys = filterNullKeys;
    }

    @Override
    public void transform(INPUT inputValue, Emitter<Tuple2<JOIN_KEY, INPUT>> emitter) throws Exception {
      JOIN_KEY key = joiner.joinOn(inputStageName, inputValue);
      if (filterNullKeys) {
        if (key == null) {
          return;
        }
        if (key instanceof StructuredRecord) {
          StructuredRecord keyRecord = (StructuredRecord) key;
          for (Schema.Field field : keyRecord.getSchema().getFields()) {
            if (keyRecord.get(field.getName()) == null) {
              return;
            }
          }
        }
      }
      emitter.emit(new Tuple2<>(key, inputValue));
    }
  }
}
