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

package co.cask.cdap.internal.app.runtime;

import java.util.concurrent.Callable;

/**
 * Interface for executing task in the form of {@link ThrowingRunnable} or {@link Callable}.
 */
public interface TaskExecutor {

  /**
   * Executes the given {@link ThrowingRunnable} task.
   *
   * @param runnable the task to be executed
   * @throws Exception if exception was raised by the {@link ThrowingRunnable#run()} method
   */
  void execute(ThrowingRunnable runnable) throws Exception;

  /**
   * Executes the given {@link Callable} task and return the result.
   *
   * @param callable the task to be executed
   * @throws Exception if exception was raised by the {@link Callable#call()} method
   */
  <T> T execute(Callable<T> callable) throws Exception;
}
