/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.audit;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.audit.AuditMessage;
import io.cdap.cdap.proto.audit.AuditPayload;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A default implementation of {@link AuditPublisher} that publishes to TMS.
 */
public final class DefaultAuditPublisher implements AuditPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuditPublisher.class);
  private static final Gson GSON = new Gson();

  private final MessagingService messagingService;
  private final TopicId auditTopic;
  private final RetryStrategy retryStrategy;

  @Inject
  DefaultAuditPublisher(CConfiguration cConf, MessagingService messagingService) {
    this.messagingService = messagingService;
    this.auditTopic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Audit.TOPIC));
    this.retryStrategy = RetryStrategies.timeLimit(
      cConf.getLong(Constants.Audit.PUBLISH_TIMEOUT_MS), TimeUnit.MILLISECONDS,
      RetryStrategies.exponentialDelay(10, 200, TimeUnit.MILLISECONDS));
  }

  @Override
  public void publish(EntityId entityId, AuditType auditType, AuditPayload auditPayload) {
    publish(entityId.toMetadataEntity(), auditType, auditPayload);
  }

  @Override
  public void publish(MetadataEntity metadataEntity, AuditType auditType, AuditPayload auditPayload) {
    String userId = Objects.firstNonNull(SecurityRequestContext.getUserId(), "");
    AuditMessage auditMessage = new AuditMessage(System.currentTimeMillis(), metadataEntity, userId,
                                                 auditType, auditPayload);
    LOG.trace("Publishing audit message {}", auditMessage);

    StoreRequest storeRequest = StoreRequestBuilder.of(auditTopic).addPayload(GSON.toJson(auditMessage)).build();
    try {
      Retries.callWithRetries(() -> messagingService.publish(storeRequest), retryStrategy, Retries.ALWAYS_TRUE);
    } catch (TopicNotFoundException e) {
      LOG.error("Missing topic for audit publish: {}", auditTopic);
    } catch (Exception e) {
      LOG.error("Got exception publishing audit message {}. Exception:", auditMessage, e);
    }
  }
}
