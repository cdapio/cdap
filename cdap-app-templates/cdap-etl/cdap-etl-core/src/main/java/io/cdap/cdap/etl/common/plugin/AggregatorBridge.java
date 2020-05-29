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

package io.cdap.cdap.etl.common.plugin;

import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;

import java.util.Iterator;

/**
 * An implementation of {@link BatchAggregator} using a {@link BatchReducibleAggregator}.
 *
 * @param <GROUP_KEY> Type of group key
 * @param <GROUP_VALUE> Type of values to group
 * @param <AGG_VALUE> Type of agg values to group
 * @param <OUT> Type of output object
 */
public class AggregatorBridge<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT>
  extends BatchAggregator<GROUP_KEY, GROUP_VALUE, OUT> {

  private final BatchReducibleAggregator<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT> aggregator;

  public AggregatorBridge(BatchReducibleAggregator<GROUP_KEY, GROUP_VALUE, AGG_VALUE, OUT> aggregator) {
    this.aggregator = aggregator;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    aggregator.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    aggregator.prepareRun(context);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    aggregator.initialize(context);
  }

  @Override
  public void destroy() {
    aggregator.destroy();
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchAggregatorContext context) {
    aggregator.onRunFinish(succeeded, context);
  }

  @Override
  public void groupBy(GROUP_VALUE groupValue, Emitter<GROUP_KEY> emitter) throws Exception {
    aggregator.groupBy(groupValue, emitter);
  }

  @Override
  public void aggregate(GROUP_KEY groupKey, Iterator<GROUP_VALUE> groupValues, Emitter<OUT> emitter) throws Exception {
    // this condition should not happen, since the group key is generated with the groupBy with existing input, so the
    // iterator will at least contain that value
    if (!groupValues.hasNext()) {
      return;
    }

    // create first agg value
    AGG_VALUE aggVal = aggregator.initializeAggregateValue(groupValues.next());

    // loop the iterator to combine the values, here the mergePartitions will not be used since the iterator already
    // has all the values with the key from all partitions
    while (groupValues.hasNext()) {
      aggVal = aggregator.mergeValues(aggVal, groupValues.next());
    }

    // after we get the final aggVal, we can finalize the result
    aggregator.finalize(groupKey, aggVal, emitter);
  }
}
