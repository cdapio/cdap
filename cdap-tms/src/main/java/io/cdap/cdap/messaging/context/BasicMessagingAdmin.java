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

package io.cdap.cdap.messaging.context;

import io.cdap.cdap.api.messaging.MessagingAdmin;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

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
  public void createTopic(String topic) throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    createTopic(topic, Collections.emptyMap());
  }

  @Override
  public void createTopic(String topic,
                          Map<String, String> properties)
    throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    messagingService.createTopic(new TopicMetadata(namespace.topic(topic), properties));
  }

  @Override
  public Map<String, String> getTopicProperties(String topic)
    throws TopicNotFoundException, IOException, UnauthorizedException {
    return messagingService.getTopic(namespace.topic(topic)).getProperties();
  }

  @Override
  public void updateTopic(String topic, Map<String, String> properties)
    throws TopicNotFoundException, IOException, UnauthorizedException {
    messagingService.updateTopic(new TopicMetadata(namespace.topic(topic), properties));
  }

  @Override
  public void deleteTopic(String topic)
    throws TopicNotFoundException, IOException, UnauthorizedException {
    messagingService.deleteTopic(namespace.topic(topic));
  }
}
