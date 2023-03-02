/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.api.service;

import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceHandler;

/**
 * Defines a custom user Service. Services are custom applications that run in program containers
 * and provide endpoints to serve requests.
 *
 * @param <T> type of service configurer
 * @param <V> type of service context
 */
public interface Service<T extends ServiceConfigurer, V extends ServiceContext> extends
    ProgramLifecycle<V> {

  /**
   * Configure the Service by adding {@link HttpServiceHandler}s to handle requests.
   *
   * @param configurer to use to add handlers to the Service.
   */
  void configure(T configurer);

  /**
   * Initialize the {@code Service}. This method will be called before the service starts up. This
   * method will be called before each service instance starts up. If there is more than one
   * instance of the service, it will be called more than once.
   *
   * Note that unlike most program types, this method is not called within an implicit transaction,
   * but instead it can start its own transactions using {@link ServiceContext#execute(TxRunnable)}.
   * methods.
   */
  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  default void initialize(V context) throws Exception {
    // no-op
  }

  /**
   * Destroy the {@code Service}. This method will be called before each service instance shuts
   * down. If there is more than one instance of the service, it will be called more than once.
   *
   * Note that unlike most program types, this method is not called within an implicit transaction,
   * but instead it can start its own transactions using {@link ServiceContext#execute(TxRunnable)}.
   */
  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  default void destroy() {
    // no-op
  }
}
