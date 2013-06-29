/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provides access to metrics about running processors.
 */
public interface BenchmarkRuntimeMetrics {
  /**
   * @return number of inputs read
   */
  long getInput();

  /**
   * @return number of inputs processed successfully.
   */
  long getProcessed();

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
}
