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
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

/**
 * Function that uses a BatchAggregator to perform the aggregate part of the aggregator.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 */
public class AggregatorAggregateFunction implements FlatMapFunction<Tuple2<Object, Iterable<Object>>, Object> {
  private final PluginFunctionContext pluginFunctionContext;
  private transient TrackedTransform<Tuple2<Object, Iterable<Object>>, Object> aggregateTransform;
  private transient DefaultEmitter<Object> emitter;

  public AggregatorAggregateFunction(PluginFunctionContext pluginFunctionContext) {
    this.pluginFunctionContext = pluginFunctionContext;
  }

  @Override
  public Iterable<Object> call(Tuple2<Object, Iterable<Object>> input) throws Exception {
    if (aggregateTransform == null) {
      BatchAggregator<Object, Object, Object> aggregator = pluginFunctionContext.createPlugin();
      aggregator.initialize(pluginFunctionContext.createBatchRuntimeContext());
      aggregateTransform = new TrackedTransform<>(new AggregateTransform<>(aggregator),
                                                  pluginFunctionContext.createStageMetrics(),
                                                  "aggregator.groups",
                                                  TrackedTransform.RECORDS_OUT, null, new Debugger() {
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
    aggregateTransform.transform(input, emitter);
    return emitter.getEntries();
  }

  private static class AggregateTransform<GROUP_KEY, GROUP_VAL, OUT_VAL>
    implements Transformation<Tuple2<GROUP_KEY, Iterable<GROUP_VAL>>, OUT_VAL> {
    private final BatchAggregator<GROUP_KEY, GROUP_VAL, OUT_VAL> aggregator;

    AggregateTransform(BatchAggregator<GROUP_KEY, GROUP_VAL, OUT_VAL> aggregator) {
      this.aggregator = aggregator;
    }

    @Override
    public void transform(Tuple2<GROUP_KEY, Iterable<GROUP_VAL>> input, Emitter<OUT_VAL> emitter) throws Exception {
      aggregator.aggregate(input._1(), input._2().iterator(), emitter);
    }
  }
}
