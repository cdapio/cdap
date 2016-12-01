/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.notification.NotificationFeedInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Implementation of the {@link NotificationFeedManager} used for testing purposes.
 */
public class NoOpNotificationFeedManager implements NotificationFeedManager {

  @Override
  public boolean createFeed(NotificationFeedInfo feed) throws NotificationFeedException {
    return true;
  }

  @Override
  public void deleteFeed(NotificationFeedId feed) throws NotificationFeedNotFoundException, NotificationFeedException {
    // No-op
  }

  @Override
  public NotificationFeedInfo getFeed(NotificationFeedId feed) throws NotificationFeedNotFoundException {
    return new NotificationFeedInfo(feed.getNamespace(), feed.getCategory(), feed.getFeed(), "");
  }

  @Override
  public List<NotificationFeedInfo> listFeeds(NamespaceId namespace) throws NotificationFeedException {
    return ImmutableList.of();
  }
}
