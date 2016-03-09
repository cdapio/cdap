/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link ExecutionFuture} with result can be set explicitly.
 */
class SettableExecutionFuture<V> extends AbstractFuture<V> implements ExecutionFuture<V> {

  private static final Logger LOG = LoggerFactory.getLogger(SettableExecutionFuture.class);
  private final AtomicBoolean done;
  private final ExecutionSparkContext context;

  SettableExecutionFuture(ExecutionSparkContext context) {
    this.done = new AtomicBoolean();
    this.context = context;
  }

  @Override
  protected boolean set(V value) {
    if (done.compareAndSet(false, true)) {
      return super.set(value);
    }
    return false;
  }

  @Override
  protected boolean setException(Throwable throwable) {
    if (done.compareAndSet(false, true)) {
      return super.setException(throwable);
    }
    return false;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (!done.compareAndSet(false, true)) {
      return false;
    }

    try {
      cancelTask();
      return super.cancel(mayInterruptIfRunning);
    } catch (Throwable t) {
      // Only log and reset state, but not propagate since Future.cancel() doesn't expect exception to be thrown.
      LOG.warn("Failed to cancel Spark execution for {}.", context, t);
      done.set(false);
      return false;
    }
  }

  @Override
  public ExecutionSparkContext getSparkContext() {
    return context;
  }

  /**
   * Will be called to cancel an executing task. Sub-class can override this method to provide
   * custom cancellation logic. This method will be called before the future changed to cancelled state.
   */
  protected void cancelTask() {
    // no-op
  }
}
