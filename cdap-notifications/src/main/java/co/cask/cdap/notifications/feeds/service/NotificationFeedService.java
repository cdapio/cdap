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

package co.cask.cdap.notifications.feeds.service;

import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;

import java.util.List;

/**
 * Service side of the {@link NotificationFeedManager}.
 */
public class NotificationFeedService implements NotificationFeedManager {
  // TODO once [CDAP-903] is completed, then creating a namespace will create a store in that namespace,
  // and any operation on that store will only be possible if it exists, hence there will be no need
  // to check for the existence of a namespace.
  // If we don't create one store per namespace, how do we check for the existence of a namespace
  // when creating a feed?
  private final NotificationFeedStore store;

  @Inject
  public NotificationFeedService(NotificationFeedStore store) {
    this.store = store;
  }

  @Override
  public boolean createFeed(Id.NotificationFeed feed) throws NotificationFeedException {
    return store.createNotificationFeed(feed) == null;
  }

  @Override
  public void deleteFeed(Id.NotificationFeed feed) throws NotificationFeedException {
    if (store.deleteNotificationFeed(feed.getId()) == null) {
      throw new NotificationFeedNotFoundException("Feed did not exist in metadata store: " + feed);
    }
  }

  @Override
  public Id.NotificationFeed getFeed(Id.NotificationFeed feed) throws NotificationFeedException {
    Id.NotificationFeed f = store.getNotificationFeed(feed.getId());
    if (f == null) {
      throw new NotificationFeedNotFoundException("Feed did not exist in metadata store: " + feed);
    }
    return f;
  }

  @Override
  public List<Id.NotificationFeed> listFeeds(Id.Namespace namespace) throws NotificationFeedException {
    return store.listNotificationFeeds(namespace);
  }
}
