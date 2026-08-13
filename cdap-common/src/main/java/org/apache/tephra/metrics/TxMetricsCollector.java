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
import org.apache.hadoop.conf.Configuration;

/**
 * Compatibility bridge for Tephra's {@link TxMetricsCollector} with Guava 30+.
 */
public class TxMetricsCollector extends AbstractIdleService implements MetricsCollector {

  public TxMetricsCollector() {
  }

  @Override
  public void gauge(String metricName, int value, String... tags) {
  }

  @Override
  public void rate(String metricName) {
  }

  @Override
  public void rate(String metricName, int count) {
  }

  @Override
  public void histogram(String metricName, int value) {
  }

  @Override
  public void configure(Configuration conf) {
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return A completed future with the running state
   */
  public ListenableFuture<Service.State> start() {
    startAsync().awaitRunning();
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return The state after starting
   */
  public Service.State startAndWait() {
    startAsync().awaitRunning();
    return state();
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return A completed future with the terminated state
   */
  public ListenableFuture<Service.State> stop() {
    stopAsync().awaitTerminated();
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return The state after stopping
   */
  public Service.State stopAndWait() {
    stopAsync().awaitTerminated();
    return state();
  }
}
