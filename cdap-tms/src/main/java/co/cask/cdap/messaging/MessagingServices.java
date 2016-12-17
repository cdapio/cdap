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

package co.cask.cdap.messaging;

import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.TopicId;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to help interact with messaging service.
 */
public final class MessagingServices {

  public static void createTopicIfNotExists(MessagingService messagingService, TopicId topicId) throws IOException {
    createTopicIfNotExists(messagingService, new TopicMetadata(topicId, Collections.emptyMap()));
  }

  public static void createTopicIfNotExists(MessagingService messagingService,
                                            TopicMetadata topicMetadata) throws IOException {
    try {
      messagingService.createTopic(topicMetadata);
    } catch (TopicAlreadyExistsException e) {
      // ok.
    }
  }

  public static void publishWithRetry(MessagingService messagingService,
                                      TopicId topicId, RetryStrategy retryStrategy,
                                      byte[]...payloads) throws TopicNotFoundException,
                                                                IOException, InterruptedException {
    int failure = 0;
    long startTime = System.currentTimeMillis();
    while (true) {
      try {
        messagingService.publish(StoreRequestBuilder.of(topicId).addPayloads(payloads).build());
        break;
      } catch (IOException e) {
        long delay = retryStrategy.nextRetry(++failure, startTime);
        if (delay < 0) {
          throw e;
        }
        if (delay > 0) {
          TimeUnit.MILLISECONDS.sleep(delay);
        }
      }
    }
  }

  private MessagingServices() {
    // no-op
  }
}
