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

import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import com.google.inject.Inject;

import java.util.List;

/**
 * Service side of the {@link NotificationFeedManager}.
 */
public class NotificationFeedService implements NotificationFeedManager {
  private final NotificationFeedStore store;

  @Inject
  public NotificationFeedService(NotificationFeedStore store) {
    this.store = store;
  }

  @Override
  public boolean createFeed(NotificationFeed feed) throws NotificationFeedException {
    return store.createNotificationFeed(feed) == null;
  }

  @Override
  public void deleteFeed(NotificationFeed feed) throws NotificationFeedException {
    if (store.deleteNotificationFeed(feed.getId()) == null) {
      throw new NotificationFeedNotFoundException("Feed did not exist in metadata store: " + feed);
    }
  }

  @Override
  public NotificationFeed getFeed(NotificationFeed feed) throws NotificationFeedException {
    NotificationFeed f = store.getNotificationFeed(feed.getId());
    if (f == null) {
      throw new NotificationFeedNotFoundException("Feed did not exist in metadata store: " + feed);
    }
    return f;
  }

  @Override
  public List<NotificationFeed> listFeeds() throws NotificationFeedException {
    return store.listNotificationFeeds();
  }
}
