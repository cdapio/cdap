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

import java.util.List;
import javax.annotation.Nullable;

/**
 * Store for {@link NotificationFeedId} objects.
 */
public interface NotificationFeedStore {

  /**
   * Creates a new Notification feed.
   *
   * @param feed {@link NotificationFeedId} representing the feed
   * @return existing {@link NotificationFeedId} if a feed with the same id already exists,
   * or null if no feed with the same id exists and the feed was created successfully
   */
  @Nullable
  NotificationFeedInfo createNotificationFeed(NotificationFeedInfo feed);

  /**
   * Retrieves a Notification feed from the metadata store.
   *
   * @param feed {@link NotificationFeedId} representing the feed
   * @return {@link NotificationFeedId} of the requested feed, or null if it was not found in the store
   */
  @Nullable
  NotificationFeedInfo getNotificationFeed(NotificationFeedId feed);

  /**
   * Deletes a Notification feed from the metadata store.
   *
   * @param feed {@link NotificationFeedId} representing the feed
   * @return {@link NotificationFeedInfo} of the feed if it was found and deleted,
   * null if the specified feed did not exist
   */
  @Nullable
  NotificationFeedInfo deleteNotificationFeed(NotificationFeedId feed);

  /**
   * Lists all registered Notification feeds for the {@code namespace}.
   *
   * @param namespace Id of the namespace to list the feeds for
   * @return a list of all registered feeds
   */
  List<NotificationFeedInfo> listNotificationFeeds(NamespaceId namespace);
}
