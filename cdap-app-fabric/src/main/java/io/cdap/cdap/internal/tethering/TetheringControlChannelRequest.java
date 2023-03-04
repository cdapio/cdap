/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import io.cdap.cdap.proto.Notification;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Sent from TetheringAgentService to TetheringServerHandler. Contains last TetheringControlMessage
 * received and list of program status update notifications.
 */
public class TetheringControlChannelRequest {

  @Nullable
  private final String lastControlMessageId;
  private final List<Notification> notificationList;

  public TetheringControlChannelRequest(@Nullable String lastControlMessageId,
      @Nullable List<Notification> notificationList) {
    this.lastControlMessageId = lastControlMessageId;
    if (notificationList == null) {
      notificationList = new ArrayList<>();
    }
    this.notificationList = notificationList;
  }

  @Nullable
  public String getLastControlMessageId() {
    return lastControlMessageId;
  }

  public List<Notification> getNotificationList() {
    return notificationList;
  }
}
