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
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsTags;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.MetricTagValue;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final Set<String> validTimeRangeParams;

  @Inject
  public MetricsClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
    this.validTimeRangeParams = ImmutableSet.of("start", "end", "aggregate", "resolution",
                                                "interpolate", "maxInterpolateGap", "count");
  }

  public MetricsClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Searches for metrics tags matching the given tags.
   *
   * @param tags the tags to match
   * @return the metrics matching the given tags
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<MetricTagValue> searchTags(Map<String, String> tags)
    throws IOException, UnauthorizedException {

    List<String> queryParts = Lists.newArrayList();
    queryParts.add("target=tag");
    addTags(tags, queryParts);

    URL url = config.resolveURLV3(String.format("metrics/search?%s", Joiner.on("&").join(queryParts)));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken());
    ObjectResponse<List<MetricTagValue>> result = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<MetricTagValue>>() { }.getType());
    return result.getResponseObject();
  }

  /**
   * Searches for metrics matching the given tags.
   *
   * @param tags the tags to match
   * @return the metrics matching the given tags
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<String> searchMetrics(Map<String, String> tags)
    throws IOException, UnauthorizedException {

    List<String> queryParts = Lists.newArrayList();
    queryParts.add("target=metric");
    addTags(tags, queryParts);

    URL url = config.resolveURLV3(String.format("metrics/search?%s", Joiner.on("&").join(queryParts)));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken());
    ObjectResponse<List<String>> result = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<String>>() { }.getType());
    return result.getResponseObject();
  }

  /**
   * Gets the value of the given metrics.
   *
   * @param tags tags for the request
   * @param metric names of the metric
   * @return values of the metrics
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public MetricQueryResult query(Map<String, String> tags, String metric)
    throws IOException, UnauthorizedException {
    return query(tags, ImmutableList.of(metric), ImmutableList.<String>of(), ImmutableMap.<String, String>of());
  }

  /**
   * Gets the value of the given metrics.
   *
   * @param tags tags for the request
   * @param metrics names of the metrics
   * @param groupBys groupBys for the request
   * @return values of the metrics
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public MetricQueryResult query(Map<String, String> tags, List<String> metrics, List<String> groupBys,
                                 @Nullable String start, @Nullable String end)
    throws IOException, UnauthorizedException {

    Map<String, String> timeRangeParams = Maps.newHashMap();
    if (start != null) {
      timeRangeParams.put("start", start);
    }
    if (end != null) {
      timeRangeParams.put("end", end);
    }
    return query(tags, metrics, groupBys, timeRangeParams);
  }

  /**
   * Gets the value of the given metrics.
   *
   * @param tags tags for the request
   * @param metrics names of the metrics
   * @param groupBys groupBys for the request
   * @return values of the metrics
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  // TODO: take in query object shared by MetricsHandler
  public MetricQueryResult query(Map<String, String> tags, List<String> metrics, List<String> groupBys,
                                 @Nullable Map<String, String> timeRangeParams)
    throws IOException, UnauthorizedException {

    List<String> queryParts = Lists.newArrayList();
    queryParts.add("target=tag");
    add("metric", metrics, queryParts);
    add("groupBy", groupBys, queryParts);
    addTags(tags, queryParts);
    addTimeRangeParametersToQuery(timeRangeParams, queryParts);

    URL url = config.resolveURLV3(String.format("metrics/query?%s", Joiner.on("&").join(queryParts)));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, MetricQueryResult.class).getResponseObject();
  }

  private void addTimeRangeParametersToQuery(Map<String, String> timeRangeParams, List<String> queryParts) {
    for (Map.Entry<String, String> entry : timeRangeParams.entrySet()) {
      if (validTimeRangeParams.contains(entry.getKey())) {
        queryParts.add(entry.getKey() + "=" + entry.getValue());
      }
    }
  }

  public RuntimeMetrics getFlowletMetrics(Id.Program flowId, String flowletId) {
    return getMetrics(MetricsTags.flowlet(flowId, flowletId),
                      Constants.Metrics.Name.Flow.FLOWLET_INPUT,
                      Constants.Metrics.Name.Flow.FLOWLET_PROCESSED,
                      Constants.Metrics.Name.Flow.FLOWLET_EXCEPTIONS);
  }

  public RuntimeMetrics getServiceMetrics(Id.Program serviceId) {
    return getMetrics(MetricsTags.service(serviceId),
                      Constants.Metrics.Name.Service.SERVICE_INPUT,
                      Constants.Metrics.Name.Service.SERVICE_PROCESSED,
                      Constants.Metrics.Name.Service.SERVICE_EXCEPTIONS);
  }

  private void add(String key, List<String> values, List<String> outQueryParts) {
    for (String value : values) {
      outQueryParts.add(key + "=" + value);
    }
  }

  private void addTags(Map<String, String> tags, List<String> outQueryParts) {
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      outQueryParts.add("tag=" + tag.getKey() + ":" + tag.getValue());
    }
  }

  /**
   * Gets the {@link RuntimeMetrics} for a particular metrics context.
   *
   * @param tags the metrics tags
   * @param inputName the metrics key for input counter
   * @param processedName the metrics key for processed counter
   * @param exceptionName the metrics key for exception counter
   * @return the {@link RuntimeMetrics}
   */
  private RuntimeMetrics getMetrics(final Map<String, String> tags, final String inputName,
                                    final String processedName, final String exceptionName) {

    return new RuntimeMetrics() {
      @Override
      public long getInput() {
        return getTotalCounter(tags, inputName);
      }

      @Override
      public long getProcessed() {
        return getTotalCounter(tags, processedName);
      }

      @Override
      public long getException() {
        return getTotalCounter(tags, exceptionName);
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
        long value = getTotalCounter(tags, name);

        // Min sleep time is 10ms, max sleep time is 1 seconds
        long sleepMillis = Math.max(10, Math.min(timeoutUnit.toMillis(timeout) / 10, TimeUnit.SECONDS.toMillis(1)));
        Stopwatch stopwatch = new Stopwatch().start();
        while (value < count && stopwatch.elapsedTime(timeoutUnit) < timeout) {
          TimeUnit.MILLISECONDS.sleep(sleepMillis);
          value = getTotalCounter(tags, name);
        }

        if (value < count) {
          throw new TimeoutException("Time limit reached. Got '" + value + "' instead of '" + count + "'");
        }
      }

      @Override
      public String toString() {
        return String.format("%s; tags=%d, processed=%d, exception=%d",
                             Joiner.on(",").withKeyValueSeparator(":").join(tags),
                             getInput(), getProcessed(), getException());
      }
    };
  }

  private long getTotalCounter(Map<String, String> tags, String metricName) {
    try {
      MetricQueryResult result = query(tags, metricName);
      if (result.getSeries().length == 0) {
        return 0;
      }

      MetricQueryResult.TimeValue[] timeValues = result.getSeries()[0].getData();
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
