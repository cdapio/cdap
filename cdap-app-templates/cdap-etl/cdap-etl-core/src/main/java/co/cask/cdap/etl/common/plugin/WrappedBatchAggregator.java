/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common.plugin;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchAggregatorContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.TypeChecker;

import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Wrapper around {@link BatchAggregator} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <GROUP_KEY> group key type. Must be a supported type
 * @param <GROUP_VALUE> group value type. Must be a supported type
 * @param <OUT> output object type
 */
public class WrappedBatchAggregator<GROUP_KEY, GROUP_VALUE, OUT> extends BatchAggregator<GROUP_KEY, GROUP_VALUE, OUT> {
  private final BatchAggregator<GROUP_KEY, GROUP_VALUE, OUT> aggregator;
  private final Caller caller;

  public WrappedBatchAggregator(BatchAggregator<GROUP_KEY, GROUP_VALUE, OUT> aggregator, Caller caller) {
    this.aggregator = aggregator;
    this.caller = caller;
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        aggregator.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void initialize(final BatchRuntimeContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        aggregator.initialize(context);
        return null;
      }
    });
  }

  @Override
  public void destroy() {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        aggregator.destroy();
        return null;
      }
    });
  }

  @Override
  public void prepareRun(final BatchAggregatorContext context) throws Exception {
    context.setGroupKeyClass(TypeChecker.getGroupKeyClass(aggregator));
    context.setGroupValueClass(TypeChecker.getGroupValueClass(aggregator));
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        aggregator.prepareRun(context);
        return null;
      }
    });
  }

  @Override
  public void onRunFinish(final boolean succeeded, final BatchAggregatorContext context) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        aggregator.onRunFinish(succeeded, context);
        return null;
      }
    });
  }

  @Override
  public void groupBy(final GROUP_VALUE groupValue, final Emitter<GROUP_KEY> emitter) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        aggregator.groupBy(groupValue, emitter);
        return null;
      }
    }, CallArgs.TRACK_TIME);
  }

  @Override
  public void aggregate(final GROUP_KEY groupKey, final Iterator<GROUP_VALUE> groupValues,
                        final Emitter<OUT> emitter) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        aggregator.aggregate(groupKey, groupValues, emitter);
        return null;
      }
    }, CallArgs.TRACK_TIME);
  }
}
