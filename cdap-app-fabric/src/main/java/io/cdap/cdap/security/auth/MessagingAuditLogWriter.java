/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Queue;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * This class receives a collection of {@link AuditLogContext} and writes them in order to a
 * messaging service / topic  ( ex - tms )
 */
public class MessagingAuditLogWriter implements AuditLogWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingAuditLogWriter.class);
  private static final Gson GSON = new Gson();

  private final String topicPrefix;
  private final int noOfTopicPartitions;
  private final int randomId;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;

  @Inject
  public MessagingAuditLogWriter(CConfiguration cConf, MessagingService messagingService) {
    this(
      messagingService,
      cConf.get(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC),
      cConf.getInt(Constants.AuditLogging.AUDIT_LOG_EVENT_NUM_PARTITIONS),
      RetryStrategies.fromConfiguration(cConf, Constants.AuditLogging.AUDIT_LOG_WRITER_RETRY_PREFIX)
    );
  }

  /**
   * Create a publisher that writes {@link AuditLogContext}s to MessagingService topics.
   */
  public MessagingAuditLogWriter(MessagingService messagingService,
                                 String topicPrefix, int numTopics, RetryStrategy retryStrategy) {
    this.messagingService = messagingService;
    this.topicPrefix = topicPrefix;
    this.noOfTopicPartitions = numTopics;
    this.retryStrategy = retryStrategy;
    this.randomId = new Random().nextInt(100);
  }

  /**
   * pushes the collection of log entry to respective messaging topic
   *
   * @param auditLogContexts
   */
  @Override
  public void publish(@Nullable Queue<AuditLogContext> auditLogContexts) throws IOException {

    if (auditLogContexts != null && auditLogContexts.isEmpty()){
      return;
    }

    TopicId topic = generateTopic();

    auditLogContexts.forEach(auditLogContext -> {
      StoreRequest storeRequest = StoreRequestBuilder.of(topic)
        .addPayload(GSON.toJson(auditLogContext))
        .build();

      try {
        Retries.runWithRetries(() -> {
          try {
            messagingService.publish(storeRequest);
          } catch (TopicNotFoundException e) {
            createTopicIfNeeded(topic);
            throw new RetryableException(e);
          }
        }, retryStrategy, Retries.ALWAYS_TRUE);
      } catch (Exception e) {
        throw new RuntimeException("Failed to publish audit log event to TMS.", e);
      }
    });
  }

  private void createTopicIfNeeded(TopicId topic) throws IOException {
    try {
      messagingService.createTopic(new DefaultTopicMetadata(topic, Collections.emptyMap()));
      LOG.info("Created topic {}", topic.getTopic());
    } catch (TopicAlreadyExistsException ex) {
      // no-op
    }
  }

  /**
   *  Every operation / event for a Call should be published to a particular topic.
   *  The Topic name is determined from which POD (using thread name) + the writer class ( using a random number for
   *  this object ). A hash code is generated using these 2 values and the topic name is chosen based on number of
   *  partitions defined for the given topic prefix.
   */
  private TopicId generateTopic() {

    //Determine a topic based on "ThreadName" + Fixed random value for this writer + No of partitions
    String hashName = Thread.currentThread().getName() + this.randomId;
    int hash = hashName.hashCode();
    int result = Math.abs(hash % noOfTopicPartitions);
    String topicName = topicPrefix + result;

    return NamespaceId.SYSTEM.topic(topicName);
  }
}
