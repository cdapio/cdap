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

import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Implementation of the {@link NotificationFeedManager} used for testing purposes.
 */
public class NoOpNotificationFeedManager implements NotificationFeedManager {

  @Override
  public boolean createFeed(NotificationFeed feed) throws NotificationFeedException {
    return true;
  }

  @Override
  public void deleteFeed(NotificationFeed feed) throws NotificationFeedException {
    // No-op
  }

  @Override
  public NotificationFeed getFeed(NotificationFeed feed) throws NotificationFeedException {
    return feed;
  }

  @Override
  public List<NotificationFeed> listFeeds() throws NotificationFeedException {
    return ImmutableList.of();
  }
}
