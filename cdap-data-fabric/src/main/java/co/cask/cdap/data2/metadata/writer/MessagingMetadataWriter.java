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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.metadata.writer.MetadataMessage.Type;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * An implementation of {@link LineageWriter} that publish lineage information to TMS.
 */
public class MessagingMetadataWriter implements MetadataWriter {

  private static final Gson GSON = new Gson();

  private final TopicId topic;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;

  @Inject
  MessagingMetadataWriter(CConfiguration cConf, MessagingService messagingService) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC));
    this.messagingService = messagingService;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metadata.");
  }

  @Override
  public void add(ProgramRunId run, MetadataEntity entity,
                  @Nullable Map<String, String> propertiesToAdd,
                  @Nullable Set<String> tagsToAdd) {
    Metadata metadata = new Metadata(propertiesToAdd != null ? propertiesToAdd : Collections.emptyMap(),
                                     tagsToAdd != null ? tagsToAdd : Collections.emptySet());
    MetadataOperation operation = new MetadataOperation(entity, MetadataOperation.Type.PUT, metadata);
    publish(run, operation);
  }

  @Override
  public void remove(ProgramRunId run, MetadataEntity entity,
                     @Nullable Set<String> propertiesToDelete,
                     @Nullable Set<String> tagsToDelete) {
    Map<String, String> props = new HashMap<>();
    if (propertiesToDelete != null) {
      propertiesToDelete.forEach(prop -> props.put(prop, ""));
    }
    Metadata metadata = new Metadata(props, tagsToDelete != null ? tagsToDelete : Collections.emptySet());
    MetadataOperation operation = new MetadataOperation(entity, MetadataOperation.Type.DELETE, metadata);
    publish(run, operation);
  }

  private void publish(ProgramRunId programRunId, MetadataOperation operation) {
    MetadataMessage message = new MetadataMessage(Type.METADATA_OPERATION, programRunId, GSON.toJsonTree(operation));
    StoreRequest request = StoreRequestBuilder.of(topic).addPayloads(GSON.toJson(message)).build();
    try {
      Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy, Retries.ALWAYS_TRUE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to publish metadata operation: " + operation, e);
    }
  }
}
