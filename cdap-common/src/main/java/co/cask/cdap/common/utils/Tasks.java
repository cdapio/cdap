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

package co.cask.cdap.common.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * General task utils.
 */
public final class Tasks {

  private Tasks() { }

  /**
   * Calls callable, waiting sleepDelay between each call,
   * until it returns the desiredValue or the timeout has passed.
   *
   * @param desiredValue the desired value to get from callable
   * @param callable the callable to check
   * @param timeout time until we timeout
   * @param timeoutUnit unit of time for timeout
   * @param sleepDelay time to wait between calls to callable
   * @param sleepDelayUnit unit of time for sleepDelay
   * @param <T> type of desiredValue
   * @throws TimeoutException if timeout has passed, but didn't get the desiredValue
   * @throws InterruptedException if something interrupted this waiting operation
   * @throws ExecutionException if there was an exception in calling the callable
   */
  public static <T> void waitFor(T desiredValue, Callable<T> callable, long timeout, TimeUnit timeoutUnit,
                                 long sleepDelay, TimeUnit sleepDelayUnit)
    throws TimeoutException, InterruptedException, ExecutionException {

    long sleepDelayMs = sleepDelayUnit.toMillis(sleepDelay);
    long startTime = System.currentTimeMillis();
    long timeoutMs = timeoutUnit.toMillis(timeout);
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        if (desiredValue.equals(callable.call())) {
          return;
        }
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
      Thread.sleep(sleepDelayMs);
    }
    throw new TimeoutException();

  }
}

