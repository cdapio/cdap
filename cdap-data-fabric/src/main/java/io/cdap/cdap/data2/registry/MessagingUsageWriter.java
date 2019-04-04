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

package io.cdap.cdap.data2.registry;

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.TopicId;

import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;

/**
 * Implementation of {@link UsageWriter} that publish to TMS.
 */
public class MessagingUsageWriter implements UsageWriter {

  private static final Gson GSON = new Gson();

  private final TopicId topic;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;

  @Inject
  MessagingUsageWriter(CConfiguration cConf, MessagingService messagingService) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC));
    this.messagingService = messagingService;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metadata.");
  }

  @Override
  public void registerAll(Iterable<? extends EntityId> users, DatasetId datasetId) {
    try {
      doRegisterAll(users, datasetId);
    } catch (Exception e) {
      throw new RuntimeException("Failed to publish usage for " + datasetId
                                   + " with owners " + Iterables.toString(users), e);
    }
  }

  @Override
  public void register(EntityId user, DatasetId datasetId) {
    if (user instanceof ProgramId) {
      // Only record usage from program
      register((ProgramId) user, datasetId);
    }
  }

  @Override
  public void register(ProgramId programId, DatasetId datasetId) {
    MetadataMessage message = new MetadataMessage(MetadataMessage.Type.USAGE, programId,
                                                  GSON.toJsonTree(new DatasetUsage(datasetId)));
    StoreRequest request = StoreRequestBuilder.of(topic).addPayload(GSON.toJson(message)).build();

    try {
      Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy, Retries.ALWAYS_TRUE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to publish usage for " + datasetId + " for program " + programId, e);
    }
  }

  private void doRegisterAll(Iterable<? extends EntityId> users, EntityId entityId) throws Exception {
    // Only record usage from program
    StoreRequest request = StoreRequestBuilder.of(topic).addPayloads(
      StreamSupport.stream(users.spliterator(), false)
        .filter(ProgramId.class::isInstance)
        .map(ProgramId.class::cast)
        .map(id -> new MetadataMessage(MetadataMessage.Type.USAGE, id, GSON.toJsonTree(new DatasetUsage(entityId))))
        .map(GSON::toJson)
        .map(s -> s.getBytes(StandardCharsets.UTF_8))
        .iterator()
    ).build();
    Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy, Retries.ALWAYS_TRUE);
  }
}
