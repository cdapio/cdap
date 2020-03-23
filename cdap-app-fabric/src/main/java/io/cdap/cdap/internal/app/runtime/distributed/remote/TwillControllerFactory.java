/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import org.apache.twill.api.TwillController;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Interface for creating instance of {@link TwillControllerFactory}.
 */
interface TwillControllerFactory {

  /**
   * Creates a new instance of {@link TwillControllerFactory}.
   *
   * @param startupTask an optional task to run to start the program execution.
   *                    This task will be executed asynchronously.
   * @param timeout the maximum amount of time for the startup task to complete
   * @param timeoutUnit the time unit for the timeout
   * @return a {@link TwillController} controlling and reflecting the state of the program execution
   */
  TwillController create(@Nullable Callable<Void> startupTask, long timeout, TimeUnit timeoutUnit);
}
