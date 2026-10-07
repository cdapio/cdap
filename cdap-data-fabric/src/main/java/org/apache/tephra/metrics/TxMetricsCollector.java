/*
 * Copyright © 2026 Cask Data, Inc.
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

package org.apache.tephra.metrics;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Workaround class compiled against Guava 32.0.0-jre to satisfy Tephra TransactionManager.
 */
public class TxMetricsCollector implements MetricsCollector {

  private final Service delegate = new AbstractIdleService() {
    @Override
    protected void startUp() {}
    @Override
    protected void shutDown() {}
  };

  public void configure(org.apache.hadoop.conf.Configuration conf) {
    // no-op
  }

  @Override
  public void gauge(String metricName, int value, String... tags) {
    // no-op
  }

  @Override
  public void histogram(String metricName, int value) {
    // no-op
  }

  @Override
  public void rate(String metricName) {
    // no-op
  }

  @Override
  public void rate(String metricName, int count) {
    // no-op
  }

  public ListenableFuture<Service.State> start() {
    delegate.startAsync();
    return Futures.immediateFuture(Service.State.RUNNING);
  }

  public ListenableFuture<Service.State> stop() {
    delegate.stopAsync();
    return Futures.immediateFuture(Service.State.TERMINATED);
  }

  public Service startAsync() {
    try {
      delegate.getClass().getMethod("startAsync").invoke(delegate);
    } catch (Exception e) {
      try {
        delegate.getClass().getMethod("start").invoke(delegate);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return this;
  }

  public Service stopAsync() {
    delegate.stopAsync();
    return this;
  }

  @Override
  public void awaitRunning() {
    delegate.awaitRunning();
  }

  @Override
  public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {
    delegate.awaitRunning(timeout, unit);
  }

  @Override
  public void awaitTerminated() {
    delegate.awaitTerminated();
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
    delegate.awaitTerminated(timeout, unit);
  }

  @Override
  public Service.State state() {
    return delegate.state();
  }

  @Override
  public boolean isRunning() {
    return delegate.isRunning();
  }

  @Override
  public Throwable failureCause() {
    return delegate.failureCause();
  }

  @Override
  public void addListener(Service.Listener listener, Executor executor) {
    delegate.addListener(listener, executor);
  }
}
