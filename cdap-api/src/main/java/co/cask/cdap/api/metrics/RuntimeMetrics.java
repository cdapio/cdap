/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.api.metrics;

import co.cask.cdap.api.annotation.Beta;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provides access to metrics about running processors.
 *
 * TODO: Support custom metrics https://issues.cask.co/browse/CDAP-765
 */
@Beta
public interface RuntimeMetrics {

  /**
   * @return number of inputs read
   */
  long getInput();

  /**
   * @return number of inputs processed successfully.
   */
  long getProcessed();

  /**
   * @return number of inputs that throws exception.
   */
  long getException();

  /**
   * Waits until at least the given number of inputs has been read.
   * @param count Number of inputs to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link java.util.concurrent.TimeUnit} for the timeout time.
   * @throws java.util.concurrent.TimeoutException If the timeout time passed and still not seeing that many count.
   */
  void waitForinput(long count, long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;

  /**
   * Waits until at least the given number of inputs has been processed.
   * @param count Number of processed to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link java.util.concurrent.TimeUnit} for the timeout time.
   * @throws java.util.concurrent.TimeoutException If the timeout time passed and still not seeing that many count.
   */
  void waitForProcessed(long count, long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;

  /**
   * Waits until at least the given number of exceptions has been raised.
   * @param count Number of exceptions to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link java.util.concurrent.TimeUnit} for the timeout time.
   * @throws java.util.concurrent.TimeoutException If the timeout time passed and still not seeing that many count.
   */
  void waitForException(long count, long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;

  /**
   * Waits until the metric value of the given metric name reached or exceeded the given count.
   *
   * @param name Name of the metric
   * @param count Minimum value to wait for
   * @param timeout Maximum time to wait for
   * @param timeoutUnit {@link java.util.concurrent.TimeUnit} for the timeout time.
   * @throws TimeoutException If the timeout time passed and still not seeing that many count.
   */
  void waitFor(String name, long count, long timeout,
               TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;
  }
