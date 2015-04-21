/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.common.metrics.MetricsContexts;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Metrics.
 */
public class MetricsClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public MetricsClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public MetricsClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * Gets the value of a particular metric.
   *
   * @param metric name of the metric
   * @param context context of the metric
   * @param groupBy how to group the metrics
   * @return value of the metric
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public MetricQueryResult query(String metric, @Nullable Map<String, String> context,
                                 @Nullable String start, @Nullable String end, @Nullable String groupBy)
    throws IOException, UnauthorizedException {

    URL url = config.resolveURLV3(String.format("metrics/query?metric=%s%s%s%s%s", metric,
                                                start == null ? "" : "&start=" + start,
                                                end == null ? "" : "&end=" + end,
                                                context == null ? "" : "&context=" + contextToPathParam(context),
                                                groupBy == null ? "" : "&groupBy=" + groupBy));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, MetricQueryResult.class).getResponseObject();
  }

  public MetricQueryResult query(String metric, Map<String, String> context)
    throws IOException, UnauthorizedException {
    return query(metric, context, null, null, null);
  }

  public MetricQueryResult query(String metric) throws IOException, UnauthorizedException {
    return query(metric, null, null, null, null);
  }

  public RuntimeMetrics getFlowletMetrics(Id.Program flowId, String flowletId) {
    return getMetrics(MetricsContexts.forFlowlet(flowId, flowletId),
                      Constants.Metrics.Name.Flow.FLOWLET_INPUT,
                      Constants.Metrics.Name.Flow.FLOWLET_PROCESSED,
                      Constants.Metrics.Name.Flow.FLOWLET_EXCEPTIONS);
  }

  public RuntimeMetrics getServiceMetrics(Id.Program serviceId) {
    return getMetrics(MetricsContexts.forService(serviceId),
                      Constants.Metrics.Name.Service.SERVICE_INPUT,
                      Constants.Metrics.Name.Service.SERVICE_PROCESSED,
                      Constants.Metrics.Name.Service.SERVICE_EXCEPTIONS);
  }

  /**
   * Gets the {@link RuntimeMetrics} for a particular metrics context.
   *
   * @param context the metrics context
   * @param inputName the metrics key for input counter
   * @param processedName the metrics key for processed counter
   * @param exceptionName the metrics key for exception counter
   * @return the {@link RuntimeMetrics}
   */
  private RuntimeMetrics getMetrics(
    final Map<String, String> context, final String inputName,
    final String processedName, final String exceptionName) {

    return new RuntimeMetrics() {
      @Override
      public long getInput() {
        return getTotalCounter(context, inputName);
      }

      @Override
      public long getProcessed() {
        return getTotalCounter(context, processedName);
      }

      @Override
      public long getException() {
        return getTotalCounter(context, exceptionName);
      }

      @Override
      public void waitForinput(long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        doWaitFor(inputName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForProcessed(long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        doWaitFor(processedName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitForException(long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        doWaitFor(exceptionName, count, timeout, timeoutUnit);
      }

      @Override
      public void waitFor(String name, long count,
                          long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
        doWaitFor(name, count, timeout, timeoutUnit);
      }

      private void doWaitFor(String name, long count, long timeout, TimeUnit timeoutUnit)
        throws TimeoutException, InterruptedException {
        long value = getTotalCounter(context, name);

        // Min sleep time is 10ms, max sleep time is 1 seconds
        long sleepMillis = Math.max(10, Math.min(timeoutUnit.toMillis(timeout) / 10, TimeUnit.SECONDS.toMillis(1)));
        Stopwatch stopwatch = new Stopwatch().start();
        while (value < count && stopwatch.elapsedTime(timeoutUnit) < timeout) {
          TimeUnit.MILLISECONDS.sleep(sleepMillis);
          value = getTotalCounter(context, name);
        }

        if (value < count) {
          throw new TimeoutException("Time limit reached. Got '" + value + "' instead of '" + count + "'");
        }
      }

      @Override
      public String toString() {
        return String.format("%s; input=%d, processed=%d, exception=%d",
                             Joiner.on(",").withKeyValueSeparator(":").join(context),
                             getInput(), getProcessed(), getException());
      }
    };
  }

  private String contextToPathParam(Map<String, String> context) {
    return Joiner.on(".").withKeyValueSeparator(".").join(context);
  }

  private long getTotalCounter(Map<String, String> context, String metricName) {
    try {
      MetricQueryResult.TimeSeries[] result = query(metricName, context).getSeries();
      if (result.length == 0) {
        return 0;
      }

      MetricQueryResult.TimeValue[] timeValues = result[0].getData();
      if (timeValues.length == 0) {
        return 0;
      }

      // since it is totals, we know there's one value only
      return timeValues[0].getValue();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
