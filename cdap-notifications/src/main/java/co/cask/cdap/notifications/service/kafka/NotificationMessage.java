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

import co.cask.cdap.proto.id.NotificationFeedId;
import com.google.gson.JsonElement;

/**
 * Message sent to TMS that contains a serialized notification.
 */
final class NotificationMessage {

  private final NotificationFeedId feedId;
  private final JsonElement notificationJson;

  NotificationMessage(NotificationFeedId feedId, JsonElement notificationJson) {
    this.feedId = feedId;
    this.notificationJson = notificationJson;
  }

  NotificationFeedId getFeedId() {
    return feedId;
  }

  JsonElement getNotificationJson() {
    return notificationJson;
  }

  @Override
  public String toString() {
    return "NotificationMessage{" +
      "feedId=" + feedId +
      ", notificationJson=" + notificationJson +
      '}';
  }
}
