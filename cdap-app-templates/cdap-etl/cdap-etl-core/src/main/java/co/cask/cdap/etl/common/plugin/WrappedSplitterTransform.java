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

import co.cask.cdap.etl.api.MultiOutputEmitter;
import co.cask.cdap.etl.api.MultiOutputPipelineConfigurer;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.TransformContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around a {@link SplitterTransform} that makes sure logging, classloading, and other pipeline capabilities
 * are setup correctly.
 *
 * @param <T> type of input record
 * @param <E> type of error records emitted. Usually the same as the input record type
 */
public class WrappedSplitterTransform<T, E> extends SplitterTransform<T, E> {
  private final SplitterTransform<T, E> transform;
  private final Caller caller;
  private final OperationTimer operationTimer;

  public WrappedSplitterTransform(SplitterTransform<T, E> transform, Caller caller,
                                  OperationTimer operationTimer) {
    this.transform = transform;
    this.caller = caller;
    this.operationTimer = operationTimer;
  }

  @Override
  public void configurePipeline(final MultiOutputPipelineConfigurer multiOutputPipelineConfigurer) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        transform.configurePipeline(multiOutputPipelineConfigurer);
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
  public void transform(final T input, final MultiOutputEmitter<E> emitter) throws Exception {
    operationTimer.start();
    try {
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          transform.transform(input, new UntimedMultiOutputEmitter<>(emitter, operationTimer));
          return null;
        }
      });
    } finally {
      operationTimer.reset();
    }
  }
}
