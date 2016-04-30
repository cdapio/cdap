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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Publish audit messages on Kafka.
 */
public class KafkaAuditPublisher implements AuditPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAuditPublisher.class);
  private static final Gson GSON = new Gson();

  private final Supplier<KafkaPublisher> publisherSupplier;
  private final String kafkaTopic;

  @Inject
  public KafkaAuditPublisher(final KafkaClient kafkaClient, CConfiguration cConf) {
    this.publisherSupplier = Suppliers.memoize(new Supplier<KafkaPublisher>() {
      @Override
      public KafkaPublisher get() {
        return kafkaClient.getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED, Compression.SNAPPY);
      }
    });
    this.kafkaTopic = cConf.get(Constants.Audit.KAFKA_TOPIC);
  }

  @Override
  public void publish(EntityId entityId, AuditType auditType, AuditPayload auditPayload) {
    String userId = Objects.firstNonNull(SecurityRequestContext.getUserId(), "");
    AuditMessage auditMessage = new AuditMessage(System.currentTimeMillis(), entityId, userId, auditType, auditPayload);
    LOG.trace("Publishing audit message {}", auditMessage);

    try {
      ByteBuffer message = Charsets.UTF_8.encode((GSON.toJson(auditMessage)));

      KafkaPublisher.Preparer preparer = publisherSupplier.get().prepare(kafkaTopic);
      preparer.add(message, entityId);
      preparer.send().get();
    } catch (Exception e) {
      LOG.error("Got exception publishing audit message {}. Exception:", auditMessage, e);
    }
  }
}
