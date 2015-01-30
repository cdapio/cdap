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

package co.cask.cdap.notifications.feeds;

import co.cask.cdap.proto.Id;

import java.util.List;

/**
 * Manager for {@link Id.NotificationFeed} objects.
 */
public interface NotificationFeedManager {

  /**
   * Create a notification feed.
   *
   * @param feed the feed to create
   * @return {@code true} if the feed was created successfully; {@code false} if the {@code feed} already exists
   * @throws NotificationFeedException if the feed has an incorrect structure;
   * for example, if it is missing a name, a namespace or a category
   */
  boolean createFeed(Id.NotificationFeed feed) throws NotificationFeedException;

  /**
   * Deletes the {@code feed} from the manager store. To determine if the feed exists,
   * the {@link Id.NotificationFeed#getId} method is used.
   *
   * @param feed the {@link Id.NotificationFeed} to delete
   * @throws NotificationFeedNotFoundException if the feed does not exist
   */
  void deleteFeed(Id.NotificationFeed feed) throws NotificationFeedException;

  /**
   * Get a {@link Id.NotificationFeed} based on the {@code feed.getId()} method of the {@code feed} argument.
   *
   * @param feed feed containing the feed Id of the feed to retrieve
   * @return {@link Id.NotificationFeed} of the feed which ID is the same as {@code feed}
   * @throws NotificationFeedNotFoundException if the feed does not exist
   */
  Id.NotificationFeed getFeed(Id.NotificationFeed feed) throws NotificationFeedException;

  /**
   * List all the {@link Id.NotificationFeed}s present in the manager store for a {@code namespace}.
   *
   * @param namespace Id of the namespace to list feeds for
   * @return all the {@link Id.NotificationFeed}s present in the manager store
   * @throws NotificationFeedException in case of unforeseen error
   */
  List<Id.NotificationFeed> listFeeds(Id.Namespace namespace) throws NotificationFeedException;
}
