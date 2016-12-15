/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.cdap;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.operations.OperationalStats;
import com.google.inject.Injector;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * {@link OperationalStats} for reporting CDAP Router requests.
 */
public class CDAPConnections extends AbstractCDAPStats implements CDAPConnectionsMXBean {

  private MetricStore metricStore;
  private long totalRequests;
  private long successful;
  private long clientErrors;
  private long serverErrors;
  private long errorLogs;
  private long warnLogs;

  @Override
  public void initialize(Injector injector) {
    metricStore = injector.getInstance(MetricStore.class);
  }

  @Override
  public String getStatType() {
    return "connections";
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
    totalRequests = getMetricValue("system.request.received");
    successful = getMetricValue("system.response.successful");
    clientErrors = getMetricValue("system.response.client-error");
    serverErrors = getMetricValue("system.response.server-error");
    errorLogs = getMetricValue("system.services.log.error");
    warnLogs = getMetricValue("system.services.log.warn");
  }

  private long getMetricValue(String metricName) {
    long currentTimeSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // we want metrics in the last hour
    long startTime = currentTimeSecs - (60 * 60);
    Collection<MetricTimeSeries> metricTimeSeries = metricStore.query(
      new MetricDataQuery(startTime, currentTimeSecs, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                          Collections.<String, String>emptyMap(), Collections.<String>emptyList())
    );
    return metricTimeSeries.isEmpty() ? 0L : metricTimeSeries.iterator().next().getTimeValues().get(0).getValue();
  }
}
