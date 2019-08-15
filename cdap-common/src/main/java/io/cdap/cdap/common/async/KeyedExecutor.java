/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.common.async;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wrapper around an ExecutorService that keeps track of submitted tasks by a key. Enforces that
 * only one runnable per key is ever running, and exposes methods to get the corresponding future for a key, and check
 * if there is a runnable for a given key.
 *
 * @param <K> type of key
 */
public class KeyedExecutor<K> {
  private final ScheduledExecutorService scheduler;
  private final ConcurrentMap<K, CompletableFuture<Void>> futures;

  public KeyedExecutor(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
    this.futures = new ConcurrentHashMap<>();
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. If there is already a task
   * running for the key, the given task will not be executed, and the existing Future for the key will be returned.
   *
   * @param key the task key
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   */
  public Future<Void> execute(K key, Runnable task) {
    // Execute the runnable once
    return submit(key, () -> {
      task.run();
      return -1;
    });
  }

  /**
   * Submits a {@link RepeatedTask} for execution and returns a Future representing that task.
   * If there is already a task running for the key, the given task will not be executed,
   * and the existing Future for the key will be returned.
   *
   * @param key the task key
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   */
  public Future<Void> submit(K key, RepeatedTask task) {
    AtomicReference<Future<?>> futureReference = new AtomicReference<>();
    AtomicBoolean cancelled = new AtomicBoolean();

    CompletableFuture<Void> future = new CompletableFuture<Void>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        // Cancel the task execution
        if (cancelled.compareAndSet(false, true)) {
          Future<?> f = futureReference.get();
          return f == null || f.cancel(mayInterruptIfRunning);
        }
        return super.cancel(mayInterruptIfRunning);
      }
    };
    CompletableFuture<Void> oldFuture = futures.putIfAbsent(key, future);
    if (oldFuture != null) {
      return oldFuture;
    }

    futureReference.set(scheduler.submit(new Runnable() {
      @Override
      public void run() {
        if (cancelled.get()) {
          return;
        }
        try {
          long delayMillis = task.executeOnce();
          if (delayMillis < 0) {
            future.complete(null);
            return;
          }
          // If the task is not cancelled, schedule the task to run again
          if (!cancelled.get()) {
            futureReference.set(scheduler.schedule(this, delayMillis, TimeUnit.MILLISECONDS));
          }
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
      }
    }));

    // When the future is done, remove it from the map
    future.handle((r, e) -> futures.remove(key, future));

    return future;

  }

  /**
   * Gets the Future for the specified key.
   *
   * @param key the key
   * @return the future for the key if it exists
   */
  public Optional<Future<Void>> getFuture(K key) {
    return Optional.ofNullable(futures.get(key));
  }

  /**
   * Calls {@link ExecutorService#shutdown()} on the underlying executor
   */
  public void shutdown() {
    scheduler.shutdown();
  }

  /**
   * Calls {@link ExecutorService#shutdownNow()} on the underlying executor
   *
   * @return tasks that were awaiting execution
   */
  public List<Runnable> shutdownNow() {
    return scheduler.shutdownNow();
  }

  /**
   * Calls {@link ExecutorService#awaitTermination(long, TimeUnit)} on the underlying executor
   *
   * @param duration maximum time to wait
   * @param timeUnit the wait time unit
   * @return true if the executor terminated, false if the timeout was hit
   * @throws InterruptedException if interrupted while waiting
   */
  public boolean awaitTermination(long duration, TimeUnit timeUnit) throws InterruptedException {
    return scheduler.awaitTermination(duration, timeUnit);
  }
}
