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
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.notification.NotificationFeedInfo;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Implementation of the {@link NotificationFeedManager} that connects to a remote feed manager service
 * through internal RESTful APIs.
 */
public class RemoteNotificationFeedManager implements NotificationFeedManager {
  private static final Gson GSON = new Gson();

  private final RemoteClient remoteClient;

  @Inject
  public RemoteNotificationFeedManager(final DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false), Constants.Gateway.API_VERSION_3);
  }

  @Override
  public boolean createFeed(NotificationFeedInfo feed) throws NotificationFeedException {
    String path = String.format("namespaces/%s/feeds/categories/%s/names/%s",
                                feed.getNamespace(), feed.getCategory(), feed.getFeed());
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, path)
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
    String path = String.format("namespaces/%s/feeds/categories/%s/names/%s",
                                feed.getNamespace(), feed.getCategory(), feed.getFeed());
    HttpResponse response = execute(remoteClient.requestBuilder(HttpMethod.DELETE, path).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotificationFeedNotFoundException(feed);
    } else if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new NotificationFeedException("Cannot delete notification feed. Reason: " + response);
    }
  }

  @Override
  public NotificationFeedInfo getFeed(NotificationFeedId feed)
    throws NotificationFeedNotFoundException, NotificationFeedException {
    String path = String.format("namespaces/%s/feeds/categories/%s/names/%s",
                                feed.getNamespace(), feed.getCategory(), feed.getFeed());
    HttpResponse response = execute(remoteClient.requestBuilder(HttpMethod.GET, path).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotificationFeedNotFoundException(feed);
    } else if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new NotificationFeedException("Cannot get notification feed. Reason: " + response);
    }
    return ObjectResponse.fromJsonBody(response, NotificationFeedInfo.class).getResponseObject();
  }

  @Override
  public List<NotificationFeedInfo> listFeeds(NamespaceId namespace) throws NotificationFeedException {
    String path = String.format("namespaces/%s/feeds", namespace.getNamespace());
    HttpResponse response = execute(remoteClient.requestBuilder(HttpMethod.GET, path).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      ObjectResponse<List<NotificationFeedInfo>> r =
        ObjectResponse.fromJsonBody(response, new TypeToken<List<NotificationFeedInfo>>() { }.getType());
      return r.getResponseObject();
    }
    throw new NotificationFeedException("Cannot list notification feeds. Reason: " + response);
  }

  private HttpResponse execute(HttpRequest request) throws NotificationFeedException {
    try {
      return remoteClient.execute(request);
    } catch (IOException e) {
      throw new NotificationFeedException(
        String.format("Error connecting to Notification Feed Service at %s while doing %s",
                      request.getURL(), request.getMethod()));
    }
  }
}
