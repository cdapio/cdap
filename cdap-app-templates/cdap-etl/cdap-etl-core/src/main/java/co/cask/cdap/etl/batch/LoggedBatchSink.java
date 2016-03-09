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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.log.LogContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link BatchSource} that makes sure logging is set up correctly.
 *
 * @param <IN> type of input
 * @param <KEY_OUT> type of output key
 * @param <VAL_OUT> type of output value
 */
public class LoggedBatchSink<IN, KEY_OUT, VAL_OUT> extends BatchSink<IN, KEY_OUT, VAL_OUT> {
  private final String name;
  private final BatchSink<IN, KEY_OUT, VAL_OUT> batchSink;

  public LoggedBatchSink(String name, BatchSink<IN, KEY_OUT, VAL_OUT> batchSink) {
    this.name = name;
    this.batchSink = batchSink;
  }

  @Override
  public void prepareRun(final BatchSinkContext context) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.prepareRun(context);
        return null;
      }
    }, name);
  }

  @Override
  public void initialize(final BatchRuntimeContext context) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.initialize(context);
        return null;
      }
    }, name);
  }

  @Override
  public void transform(final IN input,
                        final Emitter<KeyValue<KEY_OUT, VAL_OUT>> emitter) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.transform(input, emitter);
        return null;
      }
    }, name);
  }

  @Override
  public void destroy() {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.destroy();
        return null;
      }
    }, name);
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.configurePipeline(pipelineConfigurer);
        return null;
      }
    }, name);
  }

  @Override
  public void onRunFinish(final boolean succeeded, final BatchSinkContext context) {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSink.onRunFinish(succeeded, context);
        return null;
      }
    }, name);
  }
}
