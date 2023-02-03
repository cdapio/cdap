/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.cdap.app.deploy;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.artifact.AppDeletionMessage;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Class for publishing the app deletion request to tms
 */
public class AppDeletionPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(AppDeletionPublisher.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
    new GsonBuilder().registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())).create();

  private final TopicId topic;
  private final RetryStrategy retryStrategy;
  private final MessagingContext messagingContext;

  public AppDeletionPublisher(CConfiguration cConf, MessagingContext messagingContext) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.APP_DELETION_EVENT_TOPIC));
    this.retryStrategy = RetryStrategies.timeLimit(
      20, TimeUnit.SECONDS,
      RetryStrategies.exponentialDelay(10, 200, TimeUnit.MILLISECONDS));
    this.messagingContext = messagingContext;
  }

  /**
   * Remove the profile metadata for the given app id. Note that the app spec is required since we might not
   * find the program/schedule information from the app meta store since they can be deleted.
   * So we must specify app spec here to make sure their profile metadata is removed.
   *
   * @param appId entity id who emits the message
   */
  public void publishAppDeletionEvent(ApplicationId appId) {
    AppDeletionMessage message = new AppDeletionMessage(appId, GSON.toJsonTree(appId));
    LOG.trace("Deleting message: {}", message);
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
  }
}
