/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.operations.cdap;

import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.operations.OperationalStats;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link OperationalStats} for reporting Load on the CDAP Router in the past hour.
 */
public class CDAPLoad extends AbstractCDAPStats implements CDAPConnectionsMXBean {

  private static final List<String> METRICS = Arrays.asList(
      "system.request.received",
      "system.response.successful",
      "system.response.client-error",
      "system.response.server-error",
      "system.services.log.error",
      "system.services.log.warn"
  );

  private MetricsSystemClient metricsSystemClient;
  private long totalRequests;
  private long successful;
  private long clientErrors;
  private long serverErrors;
  private long errorLogs;
  private long warnLogs;

  @Override
  public void initialize(Injector injector) {
    metricsSystemClient = injector.getInstance(MetricsSystemClient.class);
  }

  @Override
  public String getStatType() {
    return "lastHourLoad";
  }

  @Override
  public long getTotalRequests() {
    return totalRequests;
  }

  @Override
  public long getSuccessful() {
    return successful;
  }

  @Override
  public long getClientErrors() {
    return clientErrors;
  }

  @Override
  public long getServerErrors() {
    return serverErrors;
  }

  @Override
  public long getErrorLogs() {
    return errorLogs;
  }

  @Override
  public long getWarnLogs() {
    return warnLogs;
  }

  @Override
  public void collect() throws IOException {
    // reset all metrics
    reset();

    int currentTimeSecs = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // We want metrics in the last hour
    int startTimeSecs = currentTimeSecs - (60 * 60);
    // We want to aggregate metrics in a sliding window of the past hour from the time the request is made.
    // To do this, query metrics with the minute resolution, so you get 60 points, then aggregate on the client side.
    // Not using hourly resolution here, because that wouldn't give aggregated metrics for the past hour, but
    // just aggregated metrics for the current hour (if called 15 mins past the hour, it will only give an aggregated
    // value for the past 15 mins).
    Map<String, String> tags = Collections.singletonMap(Constants.Metrics.Tag.NAMESPACE,
        NamespaceId.SYSTEM.getEntityName());
    Collection<MetricTimeSeries> metricTimeSeries = metricsSystemClient.query(startTimeSecs,
        currentTimeSecs, 60,
        tags, METRICS, Collections.emptySet());

    for (MetricTimeSeries metricTimeSery : metricTimeSeries) {
      switch (metricTimeSery.getMetricName()) {
        case "system.request.received":
          totalRequests = aggregateMetricValue(metricTimeSery);
          break;
        case "system.response.successful":
          successful = aggregateMetricValue(metricTimeSery);
          break;
        case "system.response.client-error":
          clientErrors = aggregateMetricValue(metricTimeSery);
          break;
        case "system.response.server-error":
          serverErrors = aggregateMetricValue(metricTimeSery);
          break;
        case "system.services.log.error":
          errorLogs = aggregateMetricValue(metricTimeSery);
          break;
        case "system.services.log.warn":
          warnLogs = aggregateMetricValue(metricTimeSery);
          break;
      }
    }
  }

  private void reset() {
    totalRequests = 0;
    successful = 0;
    clientErrors = 0;
    serverErrors = 0;
    errorLogs = 0;
    warnLogs = 0;
  }
}
