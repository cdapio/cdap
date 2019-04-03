/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.internal.app.program;

import co.cask.cdap.proto.Notification;

import java.util.Map;

/**
 * Publishing program state messages and heartbeats
 */
public interface ProgramStatePublisher {

  /**
   * Publish message which is identified by notificationType and the properties.
   *
   * @param notificationType type of the notification of the message
   * @param properties properties of the message to publish
   */
  void publish(Notification.Type notificationType, Map<String, String> properties);
}
