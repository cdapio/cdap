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

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around a {@link Transform} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <IN> type of input
 * @param <OUT> type of output
 */
public class WrappedTransform<IN, OUT> extends Transform<IN, OUT> {
  private final Transform<IN, OUT> transform;
  private final Caller caller;
  private final OperationTimer operationTimer;

  public WrappedTransform(Transform<IN, OUT> transform, Caller caller, OperationTimer operationTimer) {
    this.transform = transform;
    this.caller = caller;
    this.operationTimer = operationTimer;
  }

  @Override
  public void configurePipeline(final PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        transform.configurePipeline(pipelineConfigurer);
        return null;
      }
    });
  }

  @Override
  public void prepareRun(final StageSubmitterContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        transform.prepareRun(context);
        return null;
      }
    });
  }

  @Override
  public void onRunFinish(final boolean succeeded, final StageSubmitterContext context) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        transform.onRunFinish(succeeded, context);
        return null;
      }
    });
  }

  @Override
  public void initialize(final TransformContext context) throws Exception {
    caller.call(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        transform.initialize(context);
        return null;
      }
    });
  }

  @Override
  public void destroy() {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        transform.destroy();
        return null;
      }
    });
  }

  @Override
  public void transform(final IN input, final Emitter<OUT> emitter) throws Exception {
    operationTimer.start();
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          transform.transform(input, new UntimedEmitter<>(emitter, operationTimer));
          return null;
        }
      });
    } finally {
      operationTimer.reset();
    }
  }
}
