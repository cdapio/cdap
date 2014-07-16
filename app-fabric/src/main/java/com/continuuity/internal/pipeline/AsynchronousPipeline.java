/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Context;
import com.continuuity.pipeline.Stage;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.twill.common.Threads;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * Concrete implementation of asynchronous {@link com.continuuity.pipeline.Pipeline}.
 * <p>
 * Input from each {@link com.continuuity.pipeline.Stage} is passed to next {@link com.continuuity.pipeline.Stage}.
 * Before processing to next {@link com.continuuity.pipeline.Stage}, we wait for results
 * to be available.
 * </p>
 *
 * @param <T> Type of object produced by this pipeline.
 */
public final class AsynchronousPipeline<T> extends AbstractPipeline<T> {
  private ListeningExecutorService service;

  public AsynchronousPipeline() {
    this.service =
      MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("pipeline-executor")));
  }

  /**
   * Executes a pipeline in asynchronous mode.
   * <p>
   * Waits for the results of previous to be available to move to next
   * stage of processing.
   * </p>
   *
   * @param o argument to run the pipeline.
   */
  @SuppressWarnings("unchecked")
  @Override
  public ListenableFuture<T> execute(final Object o) {
    return service.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        Object input = o;
        Object output = null;
        for (Stage stage : getStages()) {
          Context ctx = new StageContext(input);
          stage.process(ctx);
          output = ctx.getDownStream();
          input = output;  // Output of previous stage is input to next stage.
        }
        return (T) output;
      }
    });
  }
}
