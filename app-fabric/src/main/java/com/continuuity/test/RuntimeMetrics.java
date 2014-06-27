package com.continuuity.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provides access to metrics about running processors.
 */
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
