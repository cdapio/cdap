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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Future} implementation for representing a Spark job execution, which allows cancelling the job through
 * the {@link #cancel(boolean)} method or the {@link #cancel(long, TimeUnit)} method for termination with a graceful
 * timeout. This future will complete when the job finished either normally or due to failure or cancellation.
 *
 * @param <V> type of object returned by the {@link #get()} method.
 */
public interface SparkJobFuture<V> extends Future<V> {

  /**
   * Creates a {@link SparkJobFuture} that is already cancelled.
   * @param <V>
   * @return
   */
  static <V> SparkJobFuture<V> immeidateCancelled() {
    SparkJobFuture<V> future = new AbstractSparkJobFuture<V>(0L) {

      @Override
      protected void onCancel(long timeout, TimeUnit timeoutTimeUnit) {
        // no-op
      }
    };

    future.cancel(true);
    return future;
  }

  @Override
  boolean cancel(boolean mayInterruptIfRunning);

  boolean cancel(long gracefulTimeout, TimeUnit gracefulTimeoutUnit);
}
