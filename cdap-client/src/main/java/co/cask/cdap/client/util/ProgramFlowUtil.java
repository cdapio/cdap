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

package co.cask.cdap.client.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utilities for controlling program flow.
 */
public class ProgramFlowUtil {

  private ProgramFlowUtil() { }

  /**
   * Calls callable, waiting sleepDelay between each call,
   * until it returns the desiredValue or the timeout has passed.
   *
   * @param desiredValue the desired value to get from callable
   * @param callable the callable to check
   * @param timeout time until we timeout
   * @param sleepDelay time to wait between calls to callable
   * @param timeUnit unit of time for timeout and sleepDelay
   * @param <T> type of desiredValue
   * @throws TimeoutException if timeout has passed, but didn't get the desiredValue
   * @throws InterruptedException if something interrupted this waiting operation
   * @throws ExecutionException if there was an exception in calling the callable
   */
  public static <T> void waitFor(T desiredValue, Callable<T> callable, long timeout, long sleepDelay, TimeUnit timeUnit)
    throws TimeoutException, InterruptedException, ExecutionException {

    long startTime = System.currentTimeMillis();
    long timeoutMs = timeUnit.toMillis(timeout);
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        if (desiredValue.equals(callable.call())) {
          return;
        }
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
      Thread.sleep(sleepDelay);
    }
    throw new TimeoutException();

  }
}
