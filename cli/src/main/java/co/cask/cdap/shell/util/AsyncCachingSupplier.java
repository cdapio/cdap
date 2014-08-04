/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package co.cask.cdap.shell.util;

import com.google.common.base.Supplier;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Supplier that returns the cached result, and refreshes the result when stale.
 *
 * @param <T>
 */
public class AsyncCachingSupplier<T> implements Supplier<T> {

  private static final ThreadFactory DEFAULT_THREAD_FACTORY = Executors.defaultThreadFactory();

  private final Supplier<T> supplier;
  private final long refreshRateMs;
  private final ThreadFactory threadFactory;

  private T result = null;
  private long timeGotLastResult;
  private boolean firstRequest;
  private Thread currentThread;

  /**
   * @param supplier supplier that supplies the results
   * @param refreshRateMs how often to refresh the results asynchronously, in milliseconds
   * @param threadFactory thread factory to use for getting results asynchronously
   */
  public AsyncCachingSupplier(Supplier<T> supplier, long refreshRateMs, ThreadFactory threadFactory) {
    this.supplier = supplier;
    this.threadFactory = threadFactory;
    this.firstRequest = true;
    this.refreshRateMs = refreshRateMs;
  }

  public static <T> AsyncCachingSupplier<T> of(Supplier<T> supplier, long refreshRateMs) {
    return new AsyncCachingSupplier<T>(supplier, refreshRateMs, DEFAULT_THREAD_FACTORY);
  }

  @Override
  public T get() {
    if (firstRequest) {
      // synchronous on first request
      result = supplier.get();
      firstRequest = false;
      timeGotLastResult = System.currentTimeMillis();
      return result;
    }

    tryRefresh();
    return result;
  }

  private void tryRefresh() {
    boolean currentThreadRunning = (currentThread != null && currentThread.isAlive());
    if (currentThreadRunning && System.currentTimeMillis() - timeGotLastResult >= refreshRateMs) {
      currentThread = threadFactory.newThread(new Runnable() {
        @Override
        public void run() {
          result = supplier.get();
          timeGotLastResult = System.currentTimeMillis();
        }
      });
      currentThread.start();
    }
  }
}
