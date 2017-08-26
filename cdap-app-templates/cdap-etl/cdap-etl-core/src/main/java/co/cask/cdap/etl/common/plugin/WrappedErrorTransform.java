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
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around a {@link Transform} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <IN> the type of error record
 * @param <OUT> the type of output record
 */
public class WrappedErrorTransform<IN, OUT> extends ErrorTransform<IN, OUT> {
  private final ErrorTransform<IN, OUT> transform;
  private final Caller caller;
  private final OperationTimer operationTimer;

  public WrappedErrorTransform(ErrorTransform<IN, OUT> transform, Caller caller,
                               OperationTimer operationTimer) {
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
  public void transform(final ErrorRecord<IN> input, final Emitter<OUT> emitter) throws Exception {
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
