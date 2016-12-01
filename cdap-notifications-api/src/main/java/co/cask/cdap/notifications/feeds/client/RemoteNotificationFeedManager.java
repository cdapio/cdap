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

package co.cask.cdap.notifications.feeds.client;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.notification.NotificationFeedInfo;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the {@link NotificationFeedManager} that connects to a remote feed manager service
 * through internal RESTful APIs.
 */
public class RemoteNotificationFeedManager implements NotificationFeedManager {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteNotificationFeedManager.class);
  private static final Gson GSON = new Gson();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  @Inject
  public RemoteNotificationFeedManager(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP));
      }
    });
  }

  private InetSocketAddress getServiceAddress() throws NotificationFeedException {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3L, TimeUnit.SECONDS);
    if (discoverable != null) {
      return discoverable.getSocketAddress();
    }
    throw new NotificationFeedException(
      String.format("Cannot discover service %s", Constants.Service.APP_FABRIC_HTTP));
  }

  @Override
  public boolean createFeed(NotificationFeedInfo feed) throws NotificationFeedException {
    HttpRequest request = HttpRequest.put(resolve(
      String.format("namespaces/%s/feeds/categories/%s/names/%s",
                    feed.getNamespace(), feed.getCategory(), feed.getFeed())))
      .withBody(GSON.toJson(feed)).build();
    HttpResponse response = execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return true;
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      return false;
    }
    throw new NotificationFeedException("Cannot create notification feed. Reason: " + response);
  }

  @Override
  public void deleteFeed(NotificationFeedId feed) throws NotificationFeedNotFoundException, NotificationFeedException {
    HttpResponse response = execute(HttpRequest.delete(
      resolve(String.format("namespaces/%s/feeds/categories/%s/names/%s",
                            feed.getNamespace(), feed.getCategory(), feed.getFeed()))
    ).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotificationFeedNotFoundException(feed);
    } else if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new NotificationFeedException("Cannot delete notification feed. Reason: " + response);
    }
  }

  @Override
  public NotificationFeedInfo getFeed(NotificationFeedId feed)
    throws NotificationFeedNotFoundException, NotificationFeedException {
    HttpResponse response = execute(HttpRequest.get(
      resolve(String.format("namespaces/%s/feeds/categories/%s/names/%s",
                            feed.getNamespace(), feed.getCategory(), feed.getFeed()))
    ).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotificationFeedNotFoundException(feed);
    } else if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new NotificationFeedException("Cannot get notification feed. Reason: " + response);
    }
    return ObjectResponse.fromJsonBody(response, NotificationFeedInfo.class).getResponseObject();
  }

  @Override
  public List<NotificationFeedInfo> listFeeds(NamespaceId namespace) throws NotificationFeedException {
    HttpResponse response = execute(HttpRequest.get(resolve(
      String.format("namespaces/%s/feeds", namespace.getNamespace()))).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      ObjectResponse<List<NotificationFeedInfo>> r =
        ObjectResponse.fromJsonBody(response, new TypeToken<List<NotificationFeedInfo>>() { }.getType());
      return r.getResponseObject();
    }
    throw new NotificationFeedException("Cannot list notification feeds. Reason: " + response);
  }

  private URL resolve(String resource) throws NotificationFeedException {
    InetSocketAddress addr = getServiceAddress();
    String url = String.format("http://%s:%d%s/%s", addr.getHostName(), addr.getPort(),
                               Constants.Gateway.API_VERSION_3, resource);
    LOG.trace("Notification Feed Service URL = {}", url);
    try {
      return new URL(url);
    } catch (MalformedURLException e) {
      throw new NotificationFeedException(String.format("URL %s is malformed", url));
    }
  }

  private HttpResponse execute(HttpRequest request) throws NotificationFeedException {
    try {
      return HttpRequests.execute(request, new DefaultHttpRequestConfig());
    } catch (IOException e) {
      throw new NotificationFeedException(
        String.format("Error connecting to Notification Feed Service at %s while doing %s",
                      request.getURL(), request.getMethod()));
    }
  }
}
