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

package co.cask.cdap.notifications.service.kafka;

import co.cask.cdap.notifications.feeds.NotificationFeed;
import org.apache.twill.kafka.client.TopicPartition;

/**
 * Util methods for using Notifications with Kafka.
 */
public final class KafkaNotificationUtils {

  /**
   * Map a {@link NotificationFeed} to a Kafka topic partition.
   *
   * @param feed {@link NotificationFeed} object
   * @return Kafka topic that should contain the Notifications published on the {@code feed}
   */
  public static TopicPartition getKafkaTopicPartition(NotificationFeed feed) {
    // For now, we only have a topic per feed Category.
    // Later, we may want to have multiple topics per categories, defined in cdap-site.
    // For example, we may have 10 topics for the category streams, which names would be
    // notifications-streams-1 .. notifications-streams-10.
    return new TopicPartition(String.format("notifications-%s", feed.getCategory()), 0);
  }

  /**
   * Build the key of a Kafka message based on a {@code feed}.
   *
   * @param feed {@link NotificationFeed} to get the Kafka message key of
   * @return the key of a Kafka message based on the {@code feed}
   */
  public static String getMessageKey(NotificationFeed feed) {
    return feed.getId();
  }

  /**
   * Return the {@link NotificationFeed} attached to a Kafka message containing a notification.
   *
   * @param messageKey key of a Kafka message containing a Notification
   * @return the {@link NotificationFeed} attached to a Kafka message containing a notification
   */
  public static NotificationFeed getMessageFeed(String messageKey) {
    return NotificationFeed.fromId(messageKey);
  }

  private KafkaNotificationUtils() {
  }
}
