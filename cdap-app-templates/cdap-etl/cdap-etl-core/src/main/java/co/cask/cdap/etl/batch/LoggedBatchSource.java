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
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.log.LogContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around {@link BatchSource} that makes sure logging is set up correctly.
 *
 * @param <KEY_IN> the input key type
 * @param <VAL_IN> the input value type
 * @param <OUT> the output type
 */
public class LoggedBatchSource<KEY_IN, VAL_IN, OUT> extends BatchSource<KEY_IN, VAL_IN, OUT> {
  private final String name;
  private final BatchSource<KEY_IN, VAL_IN, OUT> batchSource;

  public LoggedBatchSource(String name, BatchSource<KEY_IN, VAL_IN, OUT> batchSource) {
    this.name = name;
    this.batchSource = batchSource;
  }

  @Override
  public void prepareRun(final BatchSourceContext context) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.prepareRun(context);
        return null;
      }
    }, name);
  }

  @Override
  public void initialize(final BatchRuntimeContext context) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.initialize(context);
        return null;
      }
    }, name);
  }

  @Override
  public void transform(final KeyValue<KEY_IN, VAL_IN> input, final Emitter<OUT> emitter) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.transform(input, emitter);
        return null;
      }
    }, name);
  }

  @Override
  public void destroy() {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.destroy();
        return null;
      }
    }, name);
  }

  @Override
  public void onRunFinish(final boolean succeeded, final BatchSourceContext context) {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.onRunFinish(succeeded, context);
        return null;
      }
    }, name);
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchSource.configurePipeline(pipelineConfigurer);
        return null;
      }
    }, name);
  }
}
