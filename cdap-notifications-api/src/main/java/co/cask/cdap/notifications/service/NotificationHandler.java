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

package co.cask.cdap.notifications.service;

import co.cask.cdap.proto.Id;

import java.lang.reflect.Type;

/**
 * Notification handler passed when subscribing to a {@link Id.NotificationFeed} using
 * {@link co.cask.cdap.notifications.service.NotificationService#subscribe}.
 *
 * @param <N> Type of the Notification to handle
 */
public interface NotificationHandler<N> {

  /**
   * @return Type of the Notification this handler will handle
   */
  Type getNotificationFeedType();

  /**
   * Method called when a notification is received by this handler.
   *
   * @param notification notification received
   * @param notificationContext {@link NotificationContext} object for the notification
   */
  void received(N notification, NotificationContext notificationContext);
}
