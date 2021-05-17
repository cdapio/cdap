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

package io.cdap.cdap.internal.profile;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Class for publishing the profile metadata change request to tms
 */
public class AdminEventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(AdminEventPublisher.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
    new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())).create();

  private final TopicId topic;
  private final RetryStrategy retryStrategy;
  private final MessagingContext messagingContext;

  public AdminEventPublisher(CConfiguration cConf, MessagingContext messagingContext) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Metadata.MESSAGING_TOPIC));
    this.retryStrategy = RetryStrategies.timeLimit(
      20, TimeUnit.SECONDS,
      RetryStrategies.exponentialDelay(10, 200, TimeUnit.MILLISECONDS));
    this.messagingContext = messagingContext;
  }

  /**
   * Update the profile metadata for the affected programs/schedules, the metadata of programs/schedules will be
   * updated according to the closest level preference that contains the profile information.
   *
   * @param entityId the entity id to update metadata, all programs/schedules under this entity id will be affected
   * @param seqId the sequence id of the preferences operation
   */
  public void publishProfileAssignment(EntityId entityId, long seqId) {
    publishMessage(entityId, MetadataMessage.Type.PROFILE_ASSIGNMENT, seqId);
  }

  /**
   * Publish a message when a profile is unassigned to an entity. The metadata of the programs/schedules will be
   * reindexed to the closest level preference that contains the profile information
   *
   * @param entityId the entity id to reindex metadata, all programs/schedules under this entity id will be affected
   * @param seqId the sequence id of the preferences operation
   */
  public void publishProfileUnAssignment(EntityId entityId, long seqId) {
    publishMessage(entityId, MetadataMessage.Type.PROFILE_UNASSIGNMENT, seqId);
  }

  /**
   * Publish a message about app creation, the programs/schedules which get created will get updated with the profile
   * metadata
   *
   * @param applicationId the app id that get created
   * @param appSpec the application specification of the app
   */
  public void publishAppCreation(ApplicationId applicationId, ApplicationSpecification appSpec) {
    publishMessage(applicationId, MetadataMessage.Type.ENTITY_CREATION, appSpec);
  }

  /**
   * Publish a message about schedule creation, the schedules which get created will get updated with the profile
   * metadata
   *
   * @param scheduleId the schedule id that get created
   */
  public void publishScheduleCreation(ScheduleId scheduleId, long updatedTime) {
    publishMessage(scheduleId, MetadataMessage.Type.ENTITY_CREATION, updatedTime);
  }

  /**
   * Remove the profile metadata for the given app id. Note that the app spec is required since we might not
   * find the program/schedule information from the app meta store since they can be deleted.
   * So we must specify app spec here to make sure their profile metadata is removed.
   *
   * @param appId entity id who emits the message
   * @param appSpec the appSpec of the app
   */
  public void publishAppDeletion(ApplicationId appId, ApplicationSpecification appSpec) {
    publishMessage(appId, MetadataMessage.Type.ENTITY_DELETION, appSpec);
  }

  /**
   * Remove the profile metadata for the given schedule id. Note that the ProgramSchedule is needed since the schedule
   * might already get deleted when we process the message. So we must specify it to make sure the profile metadata is
   * removed.
   *
   * @param programSchedule the detail of the schedule that was deleted
   */
  public void publishScheduleDeletion(ProgramSchedule programSchedule) {
    publishMessage(programSchedule.getScheduleId(), MetadataMessage.Type.ENTITY_DELETION, programSchedule);
  }

  private void publishMessage(EntityId entityId, MetadataMessage.Type type,
                              Object payload) {
    MetadataMessage message = new MetadataMessage(type, entityId, GSON.toJsonTree(payload));
    LOG.trace("Publishing message: {}", message);
    try {
      Retries.supplyWithRetries(
        () -> {
          try {
            messagingContext.getMessagePublisher().publish(NamespaceId.SYSTEM.getNamespace(), topic.getTopic(),
                                                           GSON.toJson(message));
          } catch (TopicNotFoundException | ServiceUnavailableException e) {
            throw new RetryableException(e);
          } catch (IOException | AccessException e) {
            throw Throwables.propagate(e);
          }
          return null;
        },
        retryStrategy);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to publish profile metadata request for entity id %s",
                                               entityId), e);
    }
  }
}
