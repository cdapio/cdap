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
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchReduceAggregator;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.common.TypeChecker;

import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Wrapper around {@link BatchReduceAggregator} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <GROUP_KEY> group key type. Must be a supported type
 * @param <GROUP_VALUE> group value type. Must be a supported type
 * @param <OUT> output object type
 */
public class WrappedReduceAggregator<GROUP_KEY, GROUP_VALUE, OUT>
  extends BatchReduceAggregator<GROUP_KEY, GROUP_VALUE, OUT>  {
  private final BatchReduceAggregator<GROUP_KEY, GROUP_VALUE, OUT> aggregator;
  private final Caller caller;
  private final OperationTimer operationTimer;

  public WrappedReduceAggregator(BatchReduceAggregator<GROUP_KEY, GROUP_VALUE, OUT> aggregator, Caller caller,
                                 OperationTimer operationTimer) {
    this.aggregator = aggregator;
    this.caller = caller;
    this.operationTimer = operationTimer;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked((Callable<Void>) () -> {
      aggregator.configurePipeline(pipelineConfigurer);
      return null;
    });
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    caller.call((Callable<Void>) () -> {
      aggregator.initialize(context);
      return null;
    });
  }

  @Override
  public void destroy() {
    caller.callUnchecked((Callable<Void>) () -> {
      aggregator.destroy();
      return null;
    });
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) throws Exception {
    context.setGroupKeyClass(TypeChecker.getGroupKeyClass(aggregator));
    context.setGroupValueClass(TypeChecker.getGroupValueClass(aggregator));
    caller.call((Callable<Void>) () -> {
      aggregator.prepareRun(context);
      return null;
    });
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchAggregatorContext context) {
    caller.callUnchecked((Callable<Void>) () -> {
      aggregator.onRunFinish(succeeded, context);
      return null;
    });
  }

  @Override
  public void groupBy(GROUP_VALUE groupValue, Emitter<GROUP_KEY> emitter) throws Exception {
    operationTimer.start();
    try {
      caller.call((Callable<Void>) () -> {
        aggregator.groupBy(groupValue, new UntimedEmitter<>(emitter, operationTimer));
        return null;
      });
    } finally {
      operationTimer.reset();
    }
  }

  @Override
  public void aggregate(GROUP_KEY groupKey, Iterator<GROUP_VALUE> groupValues,
                        Emitter<OUT> emitter) throws Exception {
    operationTimer.start();
    try {
      caller.call((Callable<Void>) () -> {
        aggregator.aggregate(groupKey, groupValues, new UntimedEmitter<>(emitter, operationTimer));
        return null;
      });
    } finally {
      operationTimer.reset();
    }
  }

  @Override
  public GROUP_VALUE reduce(GROUP_VALUE value1, GROUP_VALUE value2) throws Exception {
    return caller.call(() -> aggregator.reduce(value1, value2));
  }
}
