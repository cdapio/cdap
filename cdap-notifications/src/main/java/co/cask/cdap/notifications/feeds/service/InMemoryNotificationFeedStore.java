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

import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.notification.NotificationFeedInfo;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link NotificationFeedStore} that keeps the feeds in memory.
 */
public class InMemoryNotificationFeedStore implements NotificationFeedStore {

  private final Map<NotificationFeedId, NotificationFeedInfo> feeds = Maps.newHashMap();

  @Nullable
  @Override
  public synchronized NotificationFeedInfo createNotificationFeed(NotificationFeedInfo feed) {
    NotificationFeedInfo existingFeed = feeds.get(feed);
    if (existingFeed != null) {
      return existingFeed;
    }
    NotificationFeedId id = new NotificationFeedId(feed.getNamespace(), feed.getCategory(), feed.getFeed());
    feeds.put(id, feed);
    return null;
  }

  @Nullable
  @Override
  public synchronized NotificationFeedInfo getNotificationFeed(NotificationFeedId feed) {
    NotificationFeedId id = new NotificationFeedId(feed.getNamespace(), feed.getCategory(), feed.getFeed());
    return feeds.get(id);
  }

  @Nullable
  @Override
  public synchronized NotificationFeedInfo deleteNotificationFeed(NotificationFeedId feed) {
    NotificationFeedId id = new NotificationFeedId(feed.getNamespace(), feed.getCategory(), feed.getFeed());
    return feeds.remove(id);
  }

  @Override
  public synchronized List<NotificationFeedInfo> listNotificationFeeds(final NamespaceId namespace) {
    Collection<NotificationFeedInfo> filter = Collections2.filter(
      feeds.values(), new Predicate<NotificationFeedInfo>() {
        @Override
        public boolean apply(@Nullable NotificationFeedInfo input) {
          return input != null && input.getNamespace().equals(namespace.getNamespace());
        }
      });
    return ImmutableList.copyOf(filter);
  }
}
