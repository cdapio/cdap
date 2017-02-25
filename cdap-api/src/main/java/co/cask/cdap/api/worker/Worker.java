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

package co.cask.cdap.api.worker;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;

/**
 * Defines a {@code Worker}.
 */
public interface Worker extends Runnable, ProgramLifecycle<WorkerContext> {

  /**
   * Configure a {@code Worker}.
   */
  void configure(WorkerConfigurer configurer);

  /**
   * Initialize the {@code Worker}.
   *
   * Note that unlike most program types, this method is not called within an implicit transaction,
   * but instead it can start its own transactions using {@link WorkerContext#execute(TxRunnable)}.
   * methods.
   */
  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  void initialize(WorkerContext context) throws Exception;

  /**
   * Destroy the {@code Worker}.
   *
   * Note that unlike most program types, this method is not called within an implicit transaction,
   * but instead it can start its own transactions using {@link WorkerContext#execute(TxRunnable)}.
   */
  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  void destroy();

  /**
   * Stop the {@code Worker}. This method will be invoked whenever the worker is externally stopped by CDAP.
   *
   * This method will be invoked from a different thread than the one calling the {@link #run()} method.
   */
  void stop();
}
