/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.metrics;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;

/**
 * Metrics Collector Class, to emit Transaction Related Metrics.
 * Note: This default implementation is a no-op and doesn't emit any metrics
 */
public class TxMetricsCollector extends AbstractIdleService implements MetricsCollector {

  @Override
  public void gauge(String metricName, int value, String... tags) {
    //no-op
  }

  @Override
  public void rate(String metricName) {
    // no-op
  }

  @Override
  public void rate(String metricName, int count) {
    // no-op
  }

  @Override
  public void histogram(String metricName, int value) {
    // no-op
  }

  @Override
  public void configure(Configuration conf) {
    // no-op
  }

  /* Service methods */

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op
  }
}
