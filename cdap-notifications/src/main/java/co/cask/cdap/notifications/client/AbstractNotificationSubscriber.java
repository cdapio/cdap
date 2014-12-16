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

import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationFeedException;
import com.google.common.collect.Maps;
import org.apache.twill.common.Cancellable;

import java.util.Map;

/**
 * Common implementation of a {@link co.cask.cdap.notifications.client.NotificationClient.Subscriber}.
 */
public abstract class AbstractNotificationSubscriber implements NotificationClient.Subscriber {
  private final NotificationFeedClient feedClient;
  private final Map<NotificationFeed, NotificationHandler> feedMap;
  private boolean isConsuming;

  /**
   * Subscribe to notifications received on a {@code feed} directly to the Notification service that handles the
   * storing of notifications.
   *
   * @param feed {@link co.cask.cdap.notifications.NotificationFeed} to subscribe to.
   */
  protected abstract void doSubscribe(NotificationFeed feed);

  /**
   * Handle the notifications received by the Notification service - the same service to which we {@link #doSubscribe}
   * communicates with.
   *
   * @return a {@link Cancellable} object to use for cancelling the notifications consumption.
   */
  protected abstract Cancellable doConsume();

  protected AbstractNotificationSubscriber(NotificationFeedClient feedClient) {
    this.feedClient = feedClient;
    this.feedMap = Maps.newConcurrentMap();
    this.isConsuming = false;
  }

  protected void startConsuming() {
    isConsuming = true;
  }

  public boolean isConsuming() {
    return isConsuming;
  }

  public Map<NotificationFeed, NotificationHandler> getFeedMap() {
    return feedMap;
  }

  @Override
  public synchronized <N> NotificationClient.Subscriber add(NotificationFeed feed, NotificationHandler<N> handler)
    throws NotificationFeedException {
    if (isConsuming) {
      throw new NotificationFeedException("Preparer is already consuming Notifications, feeds cannot be added.");
    }
    // This call will make sure that the feed exists
    feedClient.getFeed(feed);

    doSubscribe(feed);
    feedMap.put(feed, handler);
    return this;
  }

  @Override
  public synchronized Cancellable consume() {
    if (isConsuming()) {
      throw new UnsupportedOperationException("Subscriber is already consuming.");
    }
    startConsuming();
    return doConsume();
  }
}
