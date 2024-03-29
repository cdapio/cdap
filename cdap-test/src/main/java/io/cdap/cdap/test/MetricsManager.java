/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricSearchQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.RuntimeMetrics;
import io.cdap.cdap.api.metrics.TagValue;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.MetricsTags;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.id.ServiceId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Used by tests for querying metrics system.
 */
public class MetricsManager {

  private MetricStore metricStore;

  public MetricsManager(MetricStore metricStore) {
    this.metricStore = metricStore;
  }

  /**
   * query the metric store and return the Collection<MetricTimeSeries>
   *
   * @return Collection<MetricTimeSeries>
   */
  public Collection<MetricTimeSeries> query(MetricDataQuery query) {
    return metricStore.query(query);
  }

  /**
   * Search the metric store and return the collection of metric names available for the tag-values
   * in search query
   *
   * @return Collection of metric names
   */
  public Collection<String> searchMetricNames(MetricSearchQuery query) {
    return metricStore.findMetricNames(query);
  }

  /**
   * Search the metric store and return the collection of next available tags for the search query
   *
   * @return Collection of tag values
   */
  public Collection<TagValue> searchTags(MetricSearchQuery query) {
    return metricStore.findNextAvailableTags(query);
  }

  /**
   * returns service related metrics
   *
   * @return {@link io.cdap.cdap.api.metrics.RuntimeMetrics}
   */
  public RuntimeMetrics getServiceMetrics(String namespace, String applicationId,
      String serviceId) {
    ServiceId service = new ServiceId(namespace, applicationId, serviceId);
    return getMetrics(MetricsTags.service(service),
        Constants.Metrics.Name.Service.SERVICE_INPUT,
        Constants.Metrics.Name.Service.SERVICE_PROCESSED,
        Constants.Metrics.Name.Service.SERVICE_EXCEPTIONS);
  }

  /**
   * returns service handler related metrics
   *
   * @return {@link io.cdap.cdap.api.metrics.RuntimeMetrics}
   */
  public RuntimeMetrics getServiceHandlerMetrics(String namespace, String applicationId,
      String serviceId,
      String handlerId) {
    ServiceId id = new ServiceId(namespace, applicationId, serviceId);
    return getMetrics(MetricsTags.serviceHandler(id, handlerId),
        Constants.Metrics.Name.Service.SERVICE_INPUT,
        Constants.Metrics.Name.Service.SERVICE_PROCESSED,
        Constants.Metrics.Name.Service.SERVICE_EXCEPTIONS);
  }

  /**
   * get metrics total count value for a given context and metric.
   *
   * @param tags that identify a context
   * @return the total metric
   */
  public long getTotalMetric(Map<String, String> tags, String metricName) {
    MetricDataQuery query = getTotalCounterQuery(tags, metricName);
    return getSingleValueFromTotals(query);
  }

  /**
   * waitFor a metric value count for the metric identified by metricName and context.
   *
   * @param tags - context identified by tags map
   * @param count - expected metric total count value
   */
  public void waitForTotalMetricCount(Map<String, String> tags, String metricName, long count,
      long timeout,
      TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
    long value = getTotalMetric(tags, metricName);

    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = Math.max(10,
        Math.min(timeoutUnit.toMillis(timeout) / 10, TimeUnit.SECONDS.toMillis(1)));
    Stopwatch stopwatch = new Stopwatch().start();
    while (value < count && stopwatch.elapsedTime(timeoutUnit) < timeout) {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
      value = getTotalMetric(tags, metricName);
    }

    if (value < count) {
      throw new TimeoutException(
          "Time limit reached: Expected '" + count + "' but got '" + value + "'");
    }
  }

  /**
   * waitFor a metric value count for the metric identified by metricName and context.
   *
   * @param tags - context identified by tags map
   * @param count - expected metric total count value
   */
  public void waitForExactMetricCount(Map<String, String> tags, String metricName, long count,
      long timeout, TimeUnit timeoutUnit)
      throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(count, () -> getTotalMetric(tags, metricName), timeout, timeoutUnit);
  }

  /**
   * deletes all metrics
   */
  public void resetAll() {
    metricStore.deleteAll();
  }

  private RuntimeMetrics getMetrics(final Map<String, String> context,
      final String inputName,
      final String processedName,
      @Nullable final String exceptionName) {
    return new RuntimeMetrics() {
      @Override
      public long getInput() {
        return getTotalMetric(context, inputName);
      }

      @Override
      public long getProcessed() {
        return getTotalMetric(context, processedName);
      }

      @Override
      public long getException() {
        Preconditions.checkArgument(exceptionName != null, "exception count not supported");
        return getTotalMetric(context, exceptionName);
      }

      @Override
      public void waitForinput(long count, long timeout, TimeUnit timeoutUnit)
          throws TimeoutException, InterruptedException {
        waitForTotalMetricCount(context, inputName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForProcessed(long count, long timeout, TimeUnit timeoutUnit)
          throws TimeoutException, InterruptedException {
        waitForTotalMetricCount(context, processedName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForException(long count, long timeout, TimeUnit timeoutUnit)
          throws TimeoutException, InterruptedException {
        waitForTotalMetricCount(context, exceptionName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitFor(String name, long count,
          long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
        waitForTotalMetricCount(context, name, count, timeout, timeoutUnit);
      }

      @Override
      public String toString() {
        return String.format("%s; input=%d, processed=%d, exception=%d",
            Joiner.on(",").withKeyValueSeparator(":").join(context),
            getInput(), getProcessed(), getException());
      }
    };
  }

  private MetricDataQuery getTotalCounterQuery(Map<String, String> context, String metricName) {
    return new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
        context, new ArrayList<String>());
  }

  private long getSingleValueFromTotals(MetricDataQuery query) {
    try {
      Collection<MetricTimeSeries> result = metricStore.query(query);
      if (result.isEmpty()) {
        return 0;
      }
      // since it is totals query and not groupBy specified, we know there's one time series
      List<TimeValue> timeValues = result.iterator().next().getTimeValues();
      if (timeValues.isEmpty()) {
        return 0;
      }

      // since it is totals, we know there's one value only
      return timeValues.get(0).getValue();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
