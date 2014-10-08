/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.gateway;

import co.cask.cdap.common.metrics.MetricsHelper;

/**
 * Wraps {@link MetricsHelper} to propagate counter metrics collection to gatewayMetrics.
 * Does NOT change the behavior of provided {@link MetricsHelper}.
 */
public class GatewayMetricsHelperWrapper extends MetricsHelper {
  private final GatewayMetrics gatewayMetrics;

  public GatewayMetricsHelperWrapper(MetricsHelper other, GatewayMetrics gatewayMetrics) {
    super(other);
    this.gatewayMetrics = gatewayMetrics;
  }

  @Override
  protected void count(String metricWithStatus, long count) {
    super.count(metricWithStatus, count);
    // in unit-tests it can be not initialized
    if (gatewayMetrics != null) {
      gatewayMetrics.count(metricWithStatus, count);
    }
  }
}
