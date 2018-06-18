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

package co.cask.cdap.internal.profile;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.Set;

/**
 * Class for publishing the profile metadata change request to tms
 */
public class ProfileMetadataPublisher {

  private static final Gson GSON = new Gson();

  private final TopicId topic;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;

  @Inject
  ProfileMetadataPublisher(CConfiguration cConf, MessagingService messagingService) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC));
    this.messagingService = messagingService;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metadata.");
  }

  /**
   * Update the profile metadata for the affected programs/schedules, the metadata of programs/schedules will be
   * updated according to the closest level preference that contains the profile information.
   *
   * @param entityId the entity id to update metadata, all programs/schedules under this entity id will be affected
   */
  public void updateProfileMetadata(EntityId entityId) {
    publishProfileMetadata(entityId, MetadataMessage.Type.PROFILE_UPDATE, Collections.emptySet());
  }

  /**
   * Remove the profile metadata for the given entity ids. Note that the entity ids are required since we might not
   * find the program/schedule information from the app meta store since it can be deleted. So we must specify them here
   * to make sure their profile metadata is removed.
   *
   * @param entityId entity id who emits the message
   * @param entityIds the set of entities whose profile metadata needs to get removed
   */
  public void removeProfileMetadata(EntityId entityId, Set<? extends EntityId> entityIds) {
    publishProfileMetadata(entityId, MetadataMessage.Type.PROILE_REMOVE, entityIds);
  }

  private void publishProfileMetadata(EntityId entityId, MetadataMessage.Type type, Set<? extends EntityId> entityIds) {
    MetadataMessage message = new MetadataMessage(type, entityId, GSON.toJsonTree(entityIds));
    StoreRequest request = StoreRequestBuilder.of(topic).addPayloads(GSON.toJson(message)).build();
    try {
      Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy, Retries.ALWAYS_TRUE);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to publish profile metadata request for entity id %s",
                                               entityId), e);
    }
  }
}
