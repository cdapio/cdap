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

import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;

/**
 * Basic API for Tephra to support system metrics.
 */
public interface MetricsCollector extends Service {
  /**
   * Report a metric as an absolute value.
   */
  void gauge(String metricName, int value, String... tags);

  /**
   * Report a metric as a count over a given time duration.  This method uses an implicit count of 1.
   */
  void rate(String metricName);

  /**
   * Report a metric as a count over a given time duration.
   */
  void rate(String metricName, int count);

  /**
   * Report a metric calculating the distribution of the value.
   */
  void histogram(String metricName, int value);

  /**
   * Called before the collector service is started, allowing the collector to setup any
   * required configuration.
   */
  void configure(Configuration conf);
}
