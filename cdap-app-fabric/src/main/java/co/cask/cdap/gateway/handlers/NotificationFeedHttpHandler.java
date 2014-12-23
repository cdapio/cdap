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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.feeds.service.NotificationFeedService;
import co.cask.http.HttpResponder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Http handler to access the service managing {@link NotificationFeed} objects.
 * These endpoints are only reachable internally.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class NotificationFeedHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationFeedHttpHandler.class);
  private final NotificationFeedService feedService;

  @Inject
  public NotificationFeedHttpHandler(Authenticator authenticator, NotificationFeedService feedService) {
    super(authenticator);
    this.feedService = feedService;
  }

  @PUT
  @Path("/feeds/{id}")
  public void createFeed(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    try {
      try {
        NotificationFeed feed = parseBody(request, NotificationFeed.class);
        NotificationFeed combinedFeed = new NotificationFeed.Builder(NotificationFeed.fromId(id))
          .setDescription(feed == null ? null : feed.getDescription())
          .build();
        if (feedService.createFeed(combinedFeed)) {
          responder.sendStatus(HttpResponseStatus.OK);
        } else {
          LOG.trace("Notification Feed already exists.");
          responder.sendStatus(HttpResponseStatus.CONFLICT);
        }
      } catch (IllegalArgumentException e) {
        throw new NotificationFeedException(e);
      }
    } catch (NotificationFeedException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("Could not create Notification Feed. %s", e.getMessage()));
    } catch (JsonSyntaxException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid json object provided in request body.");
    } catch (IOException e) {
      LOG.error("Failed to read Notification feed request body.", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @DELETE
  @Path("/feeds/{id}")
  public void deleteFeed(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    try {
      NotificationFeed feed;
      try {
        feed = NotificationFeed.fromId(id);
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }
      feedService.deleteFeed(feed);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NotificationFeedNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (NotificationFeedException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           String.format("Could not delete Notification Feed. %s", e.getMessage()));
    }
  }

  @GET
  @Path("/feeds/{id}")
  public void getFeed(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    try {
      NotificationFeed feed;
      try {
        feed = NotificationFeed.fromId(id);
      } catch (IllegalArgumentException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, feedService.getFeed(feed));
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
  public void listFeeds(HttpRequest request, HttpResponder responder) {
    try {
      List<NotificationFeed> feeds = feedService.listFeeds();
      responder.sendJson(HttpResponseStatus.OK, feeds);
    } catch (NotificationFeedException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           String.format("Could not check subscribe permission for Notification Feed. %s",
                                         e.getMessage()));
    }
  }
}
