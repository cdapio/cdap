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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.internal.app.runtime.ThrowingRunnable;

import java.util.concurrent.Callable;

/**
 * Interface for executing invocation to user service handler.
 */
public interface ServiceTaskExecutor {

  /**
   * Executes a given {@link ThrowingRunnable} with or without a transaction.
   *
   * @param runnable the runnable to call
   * @param transactional decide whether transaction is needed or not
   * @throws Exception if there is exception, either caused by the runnable or by the transaction system.
   */
  void execute(ThrowingRunnable runnable, boolean transactional) throws Exception;

  /**
   * Executes a given {@link Callable} with or without a transaction.
   *
   * @param callable the runnable to call
   * @param transactional decide whether transaction is needed or not
   * @return the result from the {@link Callable#call()}
   * @throws Exception if there is exception, either caused by the runnable or by the transaction system.
   */
  <T> T execute(Callable<T> callable, boolean transactional) throws Exception;

  /**
   * Returns a {@link Transactional} used by this task executor for executing transactional tasks directly.
   */
  Transactional getTransactional();
}
