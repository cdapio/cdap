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

import co.cask.cdap.proto.Id;
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

  private final Map<String, Id.NotificationFeed> feeds = Maps.newHashMap();

  @Nullable
  @Override
  public synchronized Id.NotificationFeed createNotificationFeed(Id.NotificationFeed feed) {
    Id.NotificationFeed existingFeed = feeds.get(feed.getId());
    if (existingFeed != null) {
      return existingFeed;
    }
    feeds.put(feed.getId(), feed);
    return null;
  }

  @Nullable
  @Override
  public synchronized Id.NotificationFeed getNotificationFeed(String feedId) {
    return feeds.get(feedId);
  }

  @Nullable
  @Override
  public synchronized Id.NotificationFeed deleteNotificationFeed(String feedId) {
    return feeds.remove(feedId);
  }

  @Override
  public synchronized List<Id.NotificationFeed> listNotificationFeeds(final Id.Namespace namespace) {
    Collection<Id.NotificationFeed> filter = Collections2.filter(feeds.values(), new Predicate<Id.NotificationFeed>() {
      @Override
      public boolean apply(@Nullable Id.NotificationFeed input) {
        if (input != null && input.getNamespaceId().equals(namespace.getId())) {
          return true;
        }
        return false;
      }
    });
    return ImmutableList.copyOf(filter);
  }
}
