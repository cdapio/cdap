/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.writer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.codec.OperationTypeAdapter;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link LineageWriter} and {@link FieldLineageWriter} that publish lineage
 * information to TMS.
 */
public class MessagingLineageWriter implements LineageWriter, FieldLineageWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingLineageWriter.class);
  private static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization()
      .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
      .create();

  private final TopicId topic;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;
  private final int publishSizeLimit;
  private boolean isLineageWarningLogged;

  @Inject
  MessagingLineageWriter(CConfiguration cConf, MessagingService messagingService) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC));
    this.publishSizeLimit = cConf.getInt(Constants.Metadata.MESSAGING_PUBLISH_SIZE_LIMIT);
    this.messagingService = messagingService;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metadata.");
  }

  @Override
  public void addAccess(ProgramRunId programRunId, DatasetId datasetId,
      AccessType accessType, @Nullable NamespacedEntityId componentId) {
    publishLineage(programRunId, new DataAccessLineage(accessType, datasetId, componentId));
  }

  @Override
  public void write(ProgramRunId programRunId, FieldLineageInfo info) {
    MetadataMessage message = new MetadataMessage(MetadataMessage.Type.FIELD_LINEAGE, programRunId,
        GSON.toJsonTree(info));
    publish(message);
  }


  private void publishLineage(ProgramRunId programRunId, DataAccessLineage lineage) {
    MetadataMessage message = new MetadataMessage(MetadataMessage.Type.LINEAGE, programRunId,
        GSON.toJsonTree(lineage));
    publish(message);
  }

  private void publish(MetadataMessage message) {
    String messageJson = GSON.toJson(message);
    int lineageSize = messageJson.length();

    if (lineageSize > publishSizeLimit) {
      if (!isLineageWarningLogged) {
        LOG.warn(
            "Some lineage messages are not being logged as they are larger than the limit of {} bytes.",
            publishSizeLimit);
        isLineageWarningLogged = true;
      }
      LOG.trace("Size of lineage message is {} bytes, which is larger than the limit {}.\n"
          + "Therefore the lineage will not be published.", lineageSize, publishSizeLimit);
    } else {
      StoreRequest request = StoreRequestBuilder.of(topic).addPayload(messageJson).build();
      try {
        Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy,
            Retries.ALWAYS_TRUE);
      } catch (Exception e) {
        LOG.trace("Failed to publish metadata message: {}", message);
        ProgramRunId programRunId = (ProgramRunId) message.getEntityId();
        throw new RuntimeException(
            String.format("Failed to publish metadata message of type '%s' for program "
                + "run '%s'.", message.getType(), programRunId), e);
      }
    }
  }
}
