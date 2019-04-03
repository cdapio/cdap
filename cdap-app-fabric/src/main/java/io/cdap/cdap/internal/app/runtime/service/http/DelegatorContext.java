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

package co.cask.cdap.internal.app.runtime.service.http;

import org.apache.twill.common.Cancellable;

/**
 * Context object for carrying context information used by generated handler delegator classes.
 *
 * @param <T> Type of the user http service handler
 */
public interface DelegatorContext<T> {

  /**
   * Returns an instance of the user service handler.
   * Calling this method multiple times from the same thread will return
   * the same instance until {@link #capture()} is called.
   */
  T getHandler();

  /**
   * Returns an instance of {@link ServiceTaskExecutor} for the current thread.
   * Calling this method multiple times from the same thread will return the same
   * instance util {@link #capture()} is called.
   */
  ServiceTaskExecutor getServiceTaskExecutor();

  /**
   * Capture the current context. Once this method is called, the current instances of
   * {@link ServiceTaskExecutor} and {@link ServiceTaskExecutor} associated with the caller thread
   * will no longer be available through the {@link #getHandler()} or {@link #getServiceTaskExecutor()} methods.
   *
   * @return a {@link Cancellable} to release the captured context so that the {@link ServiceTaskExecutor} and the
   *         {@link ServiceTaskExecutor} will be available for the caller thread of the {@link Cancellable#cancel()}
   *         to be reused.
   */
  Cancellable capture();
}
