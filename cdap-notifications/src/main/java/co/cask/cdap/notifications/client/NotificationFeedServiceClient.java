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

package co.cask.cdap.notifications.client;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.service.NotificationFeedException;
import co.cask.cdap.notifications.service.NotificationFeedNotFoundException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Notification feed service client.
 */
public class NotificationFeedServiceClient implements NotificationFeedClient {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationFeedServiceClient.class);
  private static final Gson GSON = new Gson();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  @Inject
  public NotificationFeedServiceClient(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new TimeLimitEndpointStrategy(new RandomEndpointStrategy(
          discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP)), 3L, TimeUnit.SECONDS);
      }
    });
  }

  private InetSocketAddress getExploreServiceAddress() {
    EndpointStrategy endpointStrategy = this.endpointStrategySupplier.get();
    if (endpointStrategy == null || endpointStrategy.pick() == null) {
      String message = String.format("Cannot discover service %s", Constants.Service.APP_FABRIC_HTTP);
      LOG.debug(message);
      throw new RuntimeException(message);
    }

    return endpointStrategy.pick().getSocketAddress();
  }

  @Override
  public boolean createFeed(NotificationFeed feed) throws NotificationFeedException {
    HttpResponse response = doPut("feeds", GSON.toJson(feed), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return true;
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      return false;
    }
    throw new NotificationFeedException("Cannot create notification feed. Reason: " + getDetails(response));
  }

  @Override
  public void deleteFeed(NotificationFeed feed) throws NotificationFeedException {
    HttpResponse response = doDelete(String.format("feeds/%s", feed.getId()));
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotificationFeedNotFoundException(String.format("Notification feed %s does not exist.", feed.getId()));
    } else if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new NotificationFeedException("Cannot delete notification feed. Reason: " + getDetails(response));
    }
  }

  @Override
  public NotificationFeed getFeed(NotificationFeed feed) throws NotificationFeedException {
    HttpResponse response = doGet(String.format("feeds/%s", feed.getId()));
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotificationFeedNotFoundException(String.format("Notification feed %s does not exist.", feed.getId()));
    } else if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new NotificationFeedException("Cannot delete notification feed. Reason: " + getDetails(response));
    }
    return parseJson(response, NotificationFeed.class);
  }

  @Override
  public List<NotificationFeed> listFeeds() throws NotificationFeedException {
    HttpResponse response = doGet("feeds");
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, new TypeToken<List<NotificationFeed>>() { }.getType());
    }
    throw new NotificationFeedException("Cannot delete notification feed. Reason: " + getDetails(response));
  }

  private <T> T parseJson(HttpResponse response, Type type) throws NotificationFeedException {
    String responseString = new String(response.getResponseBody(), Charsets.UTF_8);
    try {
      return GSON.fromJson(responseString, type);
    } catch (JsonSyntaxException e) {
      String message = String.format("Cannot parse server response: %s", responseString);
      LOG.error(message, e);
      throw new NotificationFeedException(message, e);
    } catch (JsonParseException e) {
      String message = String.format("Cannot parse server response as map: %s", responseString);
      LOG.error(message, e);
      throw new NotificationFeedException(message, e);
    }
  }

  private HttpResponse doGet(String resource) throws NotificationFeedException {
    return doRequest(resource, "GET", null, null);
  }

  private HttpResponse doPost(String resource, String body, Map<String, String> headers)
    throws NotificationFeedException {
    return doRequest(resource, "POST", headers, body);
  }

  private HttpResponse doPut(String resource, String body, Map<String, String> headers)
    throws NotificationFeedException {
    return doRequest(resource, "PUT", headers, body);
  }

  private HttpResponse doDelete(String resource) throws NotificationFeedException {
    return doRequest(resource, "DELETE", null, null);
  }

  private HttpResponse doRequest(String resource, String requestMethod,
                                 @Nullable Map<String, String> headers,
                                 @Nullable String body) throws NotificationFeedException {
    Map<String, String> newHeaders = headers;
    String resolvedUrl = resolve(resource);
    try {
      URL url = new URL(resolvedUrl);
      if (body != null) {
        return HttpRequests.execute(HttpRequest.builder(HttpMethod.valueOf(requestMethod), url)
                                      .addHeaders(newHeaders).withBody(body).build());
      } else {
        return HttpRequests.execute(HttpRequest.builder(HttpMethod.valueOf(requestMethod), url)
                                      .addHeaders(newHeaders).build());
      }
    } catch (IOException e) {
      throw new NotificationFeedException(
        String.format("Error connecting to Explore Service at %s while doing %s with headers %s and body %s",
                      resolvedUrl, requestMethod,
                      newHeaders == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(newHeaders),
                      body == null ? "null" : body), e);
    }
  }

  private String getDetails(HttpResponse response) {
    return String.format("Response code: %s, message:'%s', body: '%s'",
                         response.getResponseCode(), response.getResponseMessage(),
                         response.getResponseBody() == null ?
                           "null" : new String(response.getResponseBody(), Charsets.UTF_8));

  }

  private String resolve(String resource) {
    InetSocketAddress addr = getExploreServiceAddress();
    String url = String.format("http://%s:%s%s/%s", addr.getHostName(), addr.getPort(),
                               Constants.Gateway.API_VERSION_3, resource);
    LOG.trace("Explore URL = {}", url);
    return url;
  }

}
