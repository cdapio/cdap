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

package com.continuuity.data2.transaction.metrics;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.tephra.metrics.TxMetricsCollector;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reactor implementation for TxMetricsCollector
 */
public class ReactorTxMetricsCollector extends TxMetricsCollector {
  MetricsCollector metricsCollector;
  private static final Logger LOG = LoggerFactory.getLogger(ReactorTxMetricsCollector.class);

  @Inject
  public ReactorTxMetricsCollector(MetricsCollectionService metricsCollectionService) {
    metricsCollector = metricsCollectionService.getCollector(MetricsScope.REACTOR, "transactions", "0");
  }

  @Override
  public void gauge(String metricName, int value, String...tags) {
    metricsCollector.gauge(metricName, value, tags);
  }

}
