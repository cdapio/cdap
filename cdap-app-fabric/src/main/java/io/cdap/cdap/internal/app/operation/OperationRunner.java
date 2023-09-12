/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.operation;

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.common.operation.LongRunningOperationRequest;
import java.util.concurrent.TimeUnit;

/**
 * Interface representing runner for LRO. A runner will run only one operation at a time.
 */
public interface OperationRunner<T> {

  /**
   * Run an operation in asynchronous mode.
   *
   * @param request Request for the operation run
   */
  void run(LongRunningOperationRequest<T> request)
      throws OperationTypeNotSupportedException, InvalidRequestTypeException;

  /**
   * Attempt to stop the operation gracefully within the given timeout. Depending on the
   * implementation, if the timeout reached, the running operation will be force killed.
   *
   * @param timeout the maximum time that it allows the operation to stop gracefully
   * @param timeoutUnit the {@link TimeUnit} for the {@code timeout}
   * @return A {@link ListenableFuture} that will be completed when the operation is actually stopped.
   **/
  void stop(long timeout, TimeUnit timeoutUnit) throws Exception;
}
