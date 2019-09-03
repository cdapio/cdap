/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of {@link MetricsSystemClient} that interacts with the metric system that runs remotely.
 */
public class RemoteMetricsSystemClient implements MetricsSystemClient {

  private static final Gson GSON = new Gson();
  private static final DefaultHttpRequestConfig REQUEST_CONFIG = new DefaultHttpRequestConfig(false);

  private final String adminTopic;
  private final EndpointStrategy endpointStrategy;
  private final MessagingContext messagingContext;
  private final RetryStrategy retryStrategy;

  @Inject
  RemoteMetricsSystemClient(CConfiguration cConf,
                            DiscoveryServiceClient discoveryServiceClient,
                            MessagingService messagingService) {
    this.adminTopic = cConf.get(Constants.Metrics.ADMIN_TOPIC);
    this.endpointStrategy = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.METRICS));
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metrics.");
  }

  @Override
  public void delete(MetricDeleteQuery deleteQuery) throws IOException {
    // Delete operation is asynchronous.
    MetricsAdminMessage msg = new MetricsAdminMessage(MetricsAdminMessage.Type.DELETE, GSON.toJsonTree(deleteQuery));
    String payload = GSON.toJson(msg);

    // Retry on topic not found. The system topic is automatically created by the TMS service.
    try {
      Retries.runWithRetries(
        () -> messagingContext.getMessagePublisher().publish(NamespaceId.SYSTEM.getNamespace(), adminTopic, payload),
        retryStrategy, TopicNotFoundException.class::isInstance);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Collection<MetricTimeSeries> query(int start, int end, int resolution,
                                            Map<String, String> tags,
                                            Collection<String> metrics,
                                            Collection<String> groupByTags) throws IOException {
    String metricsParam = Joiner.on("&metric=").join(metrics);
    if (!metricsParam.isEmpty()) {
      metricsParam = "&metric=" + metricsParam;
    }
    String tagsParam = Joiner.on("&tag=").withKeyValueSeparator(":").join(tags);
    if (!tagsParam.isEmpty()) {
      tagsParam = "&tag=" + tagsParam;
    }
    String groupByParam = Joiner.on("&groupBy=").join(groupByTags);
    if (!groupByParam.isEmpty()) {
      groupByParam = "&groupBy=" + groupByParam;
    }

    // Only query for aggregate metrics. Currently that's the only use case.
    String queryString = "aggregate=true&start=" + start + "&end=" + end + "&resolution=" + resolution + "s" +
                          metricsParam + tagsParam + groupByParam;

    URL url = getBaseURI().resolve("query?" + queryString).toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.post(url).build(), REQUEST_CONFIG);
    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Failed to query for metrics " + metrics + ", with tags " + tags +
                              ". Error code " + response.getResponseCode() +
                              ", message " + response.getResponseBodyAsString());
    }
    MetricQueryResult queryResult = GSON.fromJson(response.getResponseBodyAsString(), MetricQueryResult.class);
    List<MetricTimeSeries> result = new ArrayList<>();

    for (MetricQueryResult.TimeSeries timeSeries : queryResult.getSeries()) {
      List<TimeValue> timeValues = Arrays.stream(timeSeries.getData())
        .map(tv -> new TimeValue(tv.getTime(), tv.getValue()))
        .collect(Collectors.toList());
      result.add(new MetricTimeSeries(timeSeries.getMetricName(), timeSeries.getGrouping(), timeValues));
    }
    return result;
  }

  @Override
  public Collection<String> search(Map<String, String> tags) throws IOException {
    String queryString = Joiner.on("&tag=").withKeyValueSeparator(":").join(tags);
    if (!queryString.isEmpty()) {
      queryString = "&tag=" + queryString;
    }

    URL url = getBaseURI().resolve("search?target=metric" + queryString).toURL();

    HttpResponse response = HttpRequests.execute(HttpRequest.post(url).build(), REQUEST_CONFIG);
    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Failed to search for metrics names for tags " + tags +
                              ". Error code " + response.getResponseCode() +
                              ", message " + response.getResponseBodyAsString());
    }
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<List<String>>() { }.getType());
  }

  /**
   * Returns the base URI for the metrics query service based on discovery.
   */
  private URI getBaseURI() throws IOException {
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable == null) {
      discoverable = endpointStrategy.pick(2, TimeUnit.SECONDS);
      if (discoverable == null) {
        throw new IOException("Unable to discover metrics service endpoint");
      }
    }
    return URIScheme.createURI(discoverable, "%s/metrics/", Constants.Gateway.API_VERSION_3_TOKEN);
  }
}
