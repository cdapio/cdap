/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.async;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory and utility methods for dealing with {@link Executor}.
 */
public final class ExecutorUtils {

  /**
   * Creates an {@link Executor} that always create new thread to execute runnable.
   *
   * @param threadFactory thread factory for creating new thread.
   * @return a new {@link Executor} instance
   */
  public static Executor newThreadExecutor(final ThreadFactory threadFactory) {
    return new Executor() {

      final AtomicInteger id = new AtomicInteger(0);

      @Override
      public void execute(Runnable command) {
        Thread t = threadFactory.newThread(command);
        t.start();
      }
    };
  }

  private ExecutorUtils() {
  }
}
