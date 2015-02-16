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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * HTTP handler to access the service managing {@link Id.NotificationFeed} objects.
 * These endpoints are only reachable internally.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class NotificationFeedHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationFeedHttpHandler.class);

  private static final Gson GSON = new Gson();
  private final NotificationFeedManager feedManager;

  @Inject
  public NotificationFeedHttpHandler(Authenticator authenticator, NotificationFeedManager feedManager) {
    super(authenticator);
    this.feedManager = feedManager;
  }

  @PUT
  @Path("/feeds/categories/{feed-category}/names/{feed-name}")
  public void createFeed(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("feed-category") String category,
                         @PathParam("feed-name") String name) {
    try {
      Id.NotificationFeed combinedFeed;
      try {
        Id.NotificationFeed feed = parseBody(request, Id.NotificationFeed.class);
        combinedFeed = new Id.NotificationFeed.Builder()
          .setNamespaceId(namespaceId)
          .setCategory(category)
          .setName(name)
          .setDescription(feed == null ? null : feed.getDescription())
          .build();
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             String.format("Could not create Notification Feed. %s", e.getMessage()));
        return;
      }
      if (feedManager.createFeed(combinedFeed)) {
        responder.sendString(HttpResponseStatus.OK, "Notification Feed created successfully");
      } else {
        LOG.trace("Notification Feed already exists.");
        responder.sendString(HttpResponseStatus.OK, "Notification Feed already exists.");
      }
    } catch (NotificationFeedException e) {
      LOG.error("Could not create notification feed.", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } catch (JsonSyntaxException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid json object provided in request body.");
    } catch (IOException e) {
      LOG.error("Failed to read Notification feed request body.", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/feeds/categories/{feed-category}/names/{feed-name}")
  public void deleteFeed(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("feed-category") String category,
                         @PathParam("feed-name") String name) {
    try {
      Id.NotificationFeed feed;
      try {
        feed = new Id.NotificationFeed.Builder()
          .setNamespaceId(namespaceId)
          .setCategory(category)
          .setName(name)
          .build();
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }
      feedManager.deleteFeed(feed);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NotificationFeedNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (NotificationFeedException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           String.format("Could not delete Notification Feed. %s", e.getMessage()));
    }
  }

  @GET
  @Path("/feeds/categories/{feed-category}/names/{feed-name}")
  public void getFeed(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("feed-category") String category,
                      @PathParam("feed-name") String name) {
    try {
      Id.NotificationFeed feed;
      try {
        feed = new Id.NotificationFeed.Builder()
          .setNamespaceId(namespaceId)
          .setCategory(category)
          .setName(name)
          .build();
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, feedManager.getFeed(feed));
    } catch (NotificationFeedNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (NotificationFeedException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           String.format("Could not check subscribe permission for Notification Feed. %s",
                                         e.getMessage()));
    }
  }

  @GET
  @Path("/feeds")
  public void listFeeds(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId) {
    try {
      List<Id.NotificationFeed> feeds = feedManager.listFeeds(Id.Namespace.from(namespaceId));
      responder.sendJson(HttpResponseStatus.OK, feeds);
    } catch (NotificationFeedException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           String.format("Could not check subscribe permission for Notification Feed. %s",
                                         e.getMessage()));
    }
  }

  @Nullable
  private <T> T parseBody(HttpRequest request, Class<T> type) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return null;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      return GSON.fromJson(reader, type);
    } catch (JsonSyntaxException e) {
      LOG.debug("Failed to parse body on {} as {}", request.getUri(), type, e);
      throw e;
    } finally {
      reader.close();
    }
  }
}
