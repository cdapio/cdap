/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.messaging.MessagingAdmin;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A basic implementation of {@link MessagingAdmin} by delegating to {@link MessagingService}.
 */
public class BasicMessagingAdmin implements MessagingAdmin {

  private final MessagingService messagingService;
  private final NamespaceId namespace;

  public BasicMessagingAdmin(MessagingService messagingService, NamespaceId namespace) {
    this.messagingService = messagingService;
    this.namespace = namespace;
  }

  @Override
  public void createTopic(String topic) throws TopicAlreadyExistsException, IOException {
    createTopic(topic, Collections.<String, String>emptyMap());
  }

  @Override
  public void createTopic(String topic,
                          Map<String, String> properties) throws TopicAlreadyExistsException, IOException {
    messagingService.createTopic(new TopicMetadata(namespace.topic(topic), properties));
  }

  @Override
  public Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException {
    return messagingService.getTopic(namespace.topic(topic)).getProperties();
  }

  @Override
  public void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException {
    messagingService.updateTopic(new TopicMetadata(namespace.topic(topic), properties));
  }

  @Override
  public void deleteTopic(String topic) throws TopicNotFoundException, IOException {
    messagingService.deleteTopic(namespace.topic(topic));
  }
}
