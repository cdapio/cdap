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

package co.cask.cdap.data2.audit;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.client.ClientMessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Publish audit messages to TMS Audit topic.
 */
public class TMSAuditPublisher implements AuditPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(TMSAuditPublisher.class);
  private static final Gson GSON = new Gson();

  private final TopicId auditTopic;
  private final CConfiguration cConf;
  private final ClientMessagingService service;

  public TMSAuditPublisher(final ClientMessagingService service, CConfiguration cConf) {
    this.service = service;
    this.cConf = cConf;
    this.auditTopic = new TopicId(NamespaceId.SYSTEM.getNamespace(), "audit");
  }

  @Override
  public void publish(EntityId entityId, AuditType auditType, AuditPayload auditPayload) {
    String userId = Objects.firstNonNull(SecurityRequestContext.getUserId(), "");
    AuditMessage auditMessage = new AuditMessage(System.currentTimeMillis(), entityId, userId, auditType, auditPayload);
    LOG.trace("Publishing audit message {}", auditMessage);

    try {
      byte[] payload = Bytes.toBytes(GSON.toJson(auditMessage));
      service.publish(StoreRequestBuilder.of(auditTopic).addPayloads(payload).build());
    } catch (TopicNotFoundException | IOException ex) {
      LOG.error("Got exception while publishing audit message {}. Exception: ", auditMessage, ex);
    }
  }
}
