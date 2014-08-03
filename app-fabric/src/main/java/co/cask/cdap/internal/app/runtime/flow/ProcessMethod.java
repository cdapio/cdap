/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.app.queue.InputDatum;

/**
 *
 */
interface ProcessMethod<T> {

  interface ProcessResult<V> {
    V getEvent();

    boolean isSuccess();

    /**
     * Returns the failure cause if result is not success or {@code null} otherwise.
     */
    Throwable getCause();
  }

  boolean needsInput();

  /**
   * Returns the max failure retries on this process method.
   */
  int getMaxRetries();

  /**
   * Invoke the process method for the given input, using the given decoder to convert raw
   * data into event object.
   * @param input The input to process
   * @return The event being processed, regardless if invocation of process is succeeded or not.
   */
  ProcessResult<T> invoke(InputDatum<T> input);
}
