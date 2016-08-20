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

import co.cask.cdap.api.Debugger;
import co.cask.cdap.api.preview.PreviewLogger;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.TrackedTransform;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * Function that uses a BatchAggregator to perform the groupBy part of the aggregator.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 */
public class AggregatorGroupByFunction implements PairFlatMapFunction<Object, Object, Object> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<Object, Tuple2<Object, Object>> groupByFunction;
  private transient DefaultEmitter<Tuple2<Object, Object>> emitter;

  public AggregatorGroupByFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Tuple2<Object, Object>> call(Object input) throws Exception {
    if (groupByFunction == null) {
      BatchAggregator<Object, Object, Object> aggregator = pluginFunctionContext.createPlugin();
      aggregator.initialize(pluginFunctionContext.createBatchRuntimeContext());
      groupByFunction = new TrackedTransform<>(new GroupByTransform<>(aggregator),
                                               pluginFunctionContext.createStageMetrics(),
                                               TrackedTransform.RECORDS_IN,
                                               null, pluginFunctionContext.getStageName(),
                                               new Debugger() {
                                                 @Override
                                                 public boolean isPreviewEnabled() {
                                                   return false;
                                                 }

                                                 @Override
                                                 public PreviewLogger getPreviewLogger(String loggerName) {
                                                   return new PreviewLogger() {
                                                     @Override
                                                     public void log(String propertyName, Object propertyValue) {
                                                       // no-op
                                                     }
                                                   };
                                                 }
                                               });
      emitter = new DefaultEmitter<>();
    }
    emitter.reset();
    groupByFunction.transform(input, emitter);
    return emitter.getEntries();
  }

  private static class GroupByTransform<GROUP_KEY, GROUP_VAL>
    implements Transformation<GROUP_VAL, Tuple2<GROUP_KEY, GROUP_VAL>> {
    private final BatchAggregator<GROUP_KEY, GROUP_VAL, ?> aggregator;
    private final DefaultEmitter<GROUP_KEY> keyEmitter;

    GroupByTransform(BatchAggregator<GROUP_KEY, GROUP_VAL, ?> aggregator) {
      this.aggregator = aggregator;
      this.keyEmitter = new DefaultEmitter<>();
    }

    @Override
    public void transform(final GROUP_VAL inputValue, Emitter<Tuple2<GROUP_KEY, GROUP_VAL>> emitter) throws Exception {
      keyEmitter.reset();
      aggregator.groupBy(inputValue, keyEmitter);
      for (GROUP_KEY key : keyEmitter.getEntries()) {
        emitter.emit(new Tuple2<>(key, inputValue));
      }
    }
  }
}
