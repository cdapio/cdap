/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.submit;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract base implementation of {@link SparkJobFuture}. When the job execution is completed,
 * the {@link #complete(Object)}} should be called for a successful execution,
 * or call the {@link #completeExceptionally(Throwable)} for failure. To terminate the
 * job execution while it is running, call one of the {@link #cancel(boolean)} or
 * {@link #cancel(long, TimeUnit)} methods. Sub-classes should override the
 * {@link #onCancel(long, TimeUnit)} method for cancelling the execution.
 *
 * @param <V> the type of the result object
 */
public abstract class AbstractSparkJobFuture<V> implements SparkJobFuture<V> {

  private final long defaultCancelTimeoutMillis;
  private final AtomicReference<Timeout> timeout;

  // The internalFuture is for the internal state change. Any state change happens on this internalFuture, and the
  // result will be relayed to the delegateFuture
  private final CompletableFuture<V> internalFuture;
  // The delegateFuture is the future to provide the final state of this future externally.
  private final CompletableFuture<V> delegateFuture;

  /**
   * Creates an instance with the given cancel timeout as default for the {@link #cancel(boolean)} method.
   *
   * @param defaultCancelTimeoutMillis timeout in milliseconds
   */
  AbstractSparkJobFuture(long defaultCancelTimeoutMillis) {
    this.defaultCancelTimeoutMillis = defaultCancelTimeoutMillis;
    this.timeout = new AtomicReference<>();
    this.delegateFuture = new CompletableFuture<>();

    CompletableFuture<V> completion = new CompletableFuture<>();
    completion.whenComplete((result, ex) -> {
      if (ex != null) {
        if (ex instanceof CancellationException) {
          Timeout timeout = this.timeout.get();
          if (timeout == null) {
            // This shouldn't happen as the timeout is always set before completion is being cancelled
            // in the cancel(long, TimeUnit) method.
            throw new IllegalStateException("Missing graceful timeout");
          }
          onCancel(timeout.timeout, timeout.timeUnit);
          delegateFuture.cancel(true);
        } else {
          delegateFuture.completeExceptionally(ex);
        }
      } else {
        delegateFuture.complete(result);
      }
    });
    this.internalFuture = completion;
  }

  /**
   * The callback method to invoke when this future is being cancelled. This method should block until the spark
   * job was successfully cancelled (stopped).
   *
   * @param timeout the timeout allowed to cancel the spark job
   * @param timeoutTimeUnit the unit of the timeout
   */
  protected abstract void onCancel(long timeout, TimeUnit timeoutTimeUnit);

  void complete(V result) {
    internalFuture.complete(result);
  }

  void completeExceptionally(Throwable t) {
    internalFuture.completeExceptionally(t);
  }

  @Override
  public boolean cancel(long gracefulTimeout, TimeUnit gracefulTimeoutUnit) {
    this.timeout.compareAndSet(null, new Timeout(gracefulTimeout, gracefulTimeoutUnit));
    return internalFuture.cancel(true);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return cancel(defaultCancelTimeoutMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isCancelled() {
    return delegateFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return delegateFuture.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return delegateFuture.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return delegateFuture.get(timeout, unit);
  }

  private static final class Timeout {
    private final long timeout;
    private final TimeUnit timeUnit;

    private Timeout(long timeout, TimeUnit timeUnit) {
      this.timeout = timeout;
      this.timeUnit = timeUnit;
    }
  }
}
