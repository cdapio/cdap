/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.gson.Gson;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.TopicId;

import java.util.HashMap;
import java.util.Map;

/**
 * Task publisher that sends notification for a triggered schedule.
 */
public final class ScheduleTaskPublisher {

  private static final Gson GSON = new Gson();

  private final MessagingService messagingService;
  private final TopicId topicId;

  public ScheduleTaskPublisher(MessagingService messagingService, TopicId topicId) {
    this.messagingService = messagingService;
    this.topicId = topicId;
  }

  /**
   * Publish notification for the triggered schedule
   *  @param notificationType type of the notification
   * @param scheduleId       {@link ScheduleId} of the triggered schedule
   * @param systemOverrides Arguments that would be supplied as system runtime arguments for the program.
   * @param userOverrides Arguments to add to the user runtime arguments for the program.
   */
  public void publishNotification(Notification.Type notificationType, ScheduleId scheduleId,
                                  Map<String, String> systemOverrides, Map<String, String> userOverrides)
    throws Exception {

    Map<String, String> properties = new HashMap<>();
    properties.put(ProgramOptionConstants.SCHEDULE_ID, GSON.toJson(scheduleId));
    properties.put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemOverrides));
    properties.put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userOverrides));

    Notification notification = new Notification(notificationType, properties);
    messagingService.publish(StoreRequestBuilder.of(topicId).addPayload(GSON.toJson(notification)).build());
  }
}
