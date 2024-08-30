/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.cdap.api.exception.WrappedStageException;
import java.util.concurrent.Callable;

/**
 * A caller wrapper that catches exceptions thrown during execution of a callable
 * and wraps them in a {@link WrappedStageException}.
 * This class is primarily used to associate the exception with a specific stage name in a pipeline,
 * helping in better debugging and error tracking.
 *
 * <p>
 * The class delegates the actual calling operation to another {@link Caller} instance and
 * ensures that any exceptions thrown are caught and rethrown as a {@link WrappedStageException},
 * which includes the stage name where the error occurred.
 * </p>
 */
public class ExceptionWrappingCaller extends Caller {
  private final Caller delegate;
  private final String stageName;

  /**
   * Constructs an ExceptionWrappingCaller.
   *
   * @param delegate The {@link Caller} instance that performs the actual calling of the callable.
   * @param stageName The name of the stage associated with the exception,
   *                  for easier identification of where the error occurred.
   */
  public ExceptionWrappingCaller(Caller delegate, String stageName) {
    this.delegate = delegate;
    this.stageName = stageName;
  }

  /**
   * Executes the given {@link Callable}, wrapping any exceptions thrown
   * during execution in a {@link WrappedStageException}.
   *
   * @param callable The callable task to execute.
   * @param <T> The return type of the callable.
   * @return The result of the callable task.
   * @throws Exception if an exception occurs during the callable execution.
   */
  @Override
  public <T> T call(Callable<T> callable) throws Exception {
    try {
      return delegate.call(callable);
    } catch (Exception e) {
      throw new WrappedStageException(e, stageName);
    }
  }
}
