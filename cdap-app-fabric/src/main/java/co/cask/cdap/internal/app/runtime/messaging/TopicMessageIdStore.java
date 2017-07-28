/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.messaging;

import javax.annotation.Nullable;

/**
 * Defines the interface for retrieving and persisting topic - messageId key-value pairs from storage.
 */
public interface TopicMessageIdStore {
  /**
   * Gets the messageId that was previously set for a given topic.
   *
   * @param topic the topic to lookup the last message id
   *
   * @return the messageId, or null if no messageId was previously associated with the given topic
   */
  @Nullable
  String retrieveSubscriberState(String topic);

  /**
   * Sets a given messageId to be associated with a given topic.
   *
   * @param topic the topic to persist the message id
   * @param messageId the message id
   */
  void persistSubscriberState(String topic, String messageId);
}
