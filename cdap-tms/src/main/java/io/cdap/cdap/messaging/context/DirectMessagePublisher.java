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

import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.Iterator;

/**
 * A implementation of {@link MessagePublisher} that talks to {@link MessagingService} directly.
 */
final class DirectMessagePublisher extends AbstractMessagePublisher {

  private final MessagingService messagingService;

  DirectMessagePublisher(MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @Override
  public void publish(TopicId topicId, Iterator<byte[]> payloads)
    throws IOException, TopicNotFoundException, UnauthorizedException {
    messagingService.publish(StoreRequestBuilder.of(topicId).addPayloads(payloads).build());
  }
}
