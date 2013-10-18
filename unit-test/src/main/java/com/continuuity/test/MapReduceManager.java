package com.continuuity.test;

import com.continuuity.api.mapreduce.MapReduce;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Instance for this class is for managing a running {@link MapReduce}.
 */
public interface MapReduceManager {
  /**
   * Stops the running mapreduce job.
   */
  void stop();

  /**
   * Blocks until mapreduce job is finished or given timeout is reached
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws java.util.concurrent.TimeoutException if timeout reached
   * @throws InterruptedException if execution is interrupted
   */
  void waitForFinish(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;
}
