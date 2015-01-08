/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.gson.JsonElement;

/**
 * Message sent to Kafka that contains a serialized notification.
 */
class KafkaMessage {
  private final String messageKey;
  private final JsonElement notificationJson;

  public KafkaMessage(String messageKey, JsonElement notificationJson) {
    this.messageKey = messageKey;
    this.notificationJson = notificationJson;
  }

  public String getMessageKey() {
    return messageKey;
  }

  public JsonElement getNotificationJson() {
    return notificationJson;
  }
}
