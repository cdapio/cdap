/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.app.queue.InputDatum;

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
