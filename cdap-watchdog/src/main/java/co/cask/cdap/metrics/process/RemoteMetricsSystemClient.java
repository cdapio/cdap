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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsSystemClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
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
    HttpResponse response = HttpRequests.execute(HttpRequest.post(url).build());
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

    HttpResponse response = HttpRequests.execute(HttpRequest.post(url).build());
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
    InetSocketAddress addr = discoverable.getSocketAddress();
    return URI.create(String.format("http://%s:%d/%s/metrics/",
                                    addr.getHostName(), addr.getPort(), Constants.Gateway.API_VERSION_3_TOKEN));
  }
}
