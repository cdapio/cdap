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
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link BatchSource} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <IN> type of input
 * @param <KEY_OUT> type of output key
 * @param <VAL_OUT> type of output value
 */
public class WrappedBatchSink<IN, KEY_OUT, VAL_OUT> extends BatchSink<IN, KEY_OUT, VAL_OUT> {
  private final BatchSink<IN, KEY_OUT, VAL_OUT> batchSink;
  private final Caller caller;

  public WrappedBatchSink(BatchSink<IN, KEY_OUT, VAL_OUT> batchSink, Caller caller) {
    this.batchSink = batchSink;
    this.caller = caller;
  }

  @Override
  public void prepareRun(final BatchSinkContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.prepareRun(context);
        return null;
      }
    });
  }

  @Override
  public void initialize(final BatchRuntimeContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.initialize(context);
        return null;
      }
    });
  }

  @Override
  public void transform(final IN input,
                        final Emitter<KeyValue<KEY_OUT, VAL_OUT>> emitter) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.transform(input, emitter);
        return null;
      }
    }, CallArgs.TRACK_TIME);
  }

  @Override
  public void destroy() {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.destroy();
        return null;
      }
    });
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void onRunFinish(final boolean succeeded, final BatchSinkContext context) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.onRunFinish(succeeded, context);
        return null;
      }
    });
  }
}
