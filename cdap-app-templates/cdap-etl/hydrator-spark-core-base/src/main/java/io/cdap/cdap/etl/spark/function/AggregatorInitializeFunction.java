/*
 * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import org.apache.spark.api.java.function.Function;

/**
 * Function that uses a BatchReducibleAggregator to initialize the aggregated value.
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <GROUP_VALUE> type of group value
 * @param <AGG_VALUE> type of the agg value
 */
public class AggregatorInitializeFunction<GROUP_VALUE, AGG_VALUE> implements Function<GROUP_VALUE, AGG_VALUE> {

  private final PluginFunctionContext pluginFunctionContext;
  private final FunctionCache functionCache;
  private transient BatchReducibleAggregator<?, GROUP_VALUE, AGG_VALUE, ?> aggregator;

  public AggregatorInitializeFunction(PluginFunctionContext pluginFunctionContext, FunctionCache functionCache) {
    this.pluginFunctionContext = pluginFunctionContext;
    this.functionCache = functionCache;
  }

  @Override
  public AGG_VALUE call(GROUP_VALUE value) throws Exception {
    if (aggregator == null) {
      aggregator = pluginFunctionContext.createAndInitializePlugin(functionCache);
    }
    return aggregator.initializeAggregateValue(value);
  }
}
