/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link BatchSource} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <KEY_IN> the input key type
 * @param <VAL_IN> the input value type
 * @param <OUT> the output type
 */
public class WrappedBatchSource<KEY_IN, VAL_IN, OUT> extends BatchSource<KEY_IN, VAL_IN, OUT> {
  private final BatchSource<KEY_IN, VAL_IN, OUT> batchSource;
  private final Caller caller;
  private final OperationTimer operationTimer;

  public WrappedBatchSource(BatchSource<KEY_IN, VAL_IN, OUT> batchSource,
                            Caller caller, OperationTimer operationTimer) {
    this.batchSource = batchSource;
    this.caller = caller;
    this.operationTimer = operationTimer;
  }

  @Override
  public void prepareRun(final BatchSourceContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.prepareRun(context);
        return null;
      }
    });
  }

  @Override
  public void initialize(final BatchRuntimeContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.initialize(context);
        return null;
      }
    });
  }

  @Override
  public void transform(final KeyValue<KEY_IN, VAL_IN> input, final Emitter<OUT> emitter) throws Exception {
    operationTimer.start();
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          batchSource.transform(input, new UntimedEmitter<>(emitter, operationTimer));
          return null;
        }
      });
    } finally {
      operationTimer.reset();
    }
  }

  @Override
  public void destroy() {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.destroy();
        return null;
      }
    });
  }

  @Override
  public void onRunFinish(final boolean succeeded, final BatchSourceContext context) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.onRunFinish(succeeded, context);
        return null;
      }
    });
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }
}
