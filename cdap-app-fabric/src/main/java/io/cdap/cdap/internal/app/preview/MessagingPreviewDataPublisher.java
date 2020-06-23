/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewDataPublisher;
import io.cdap.cdap.app.preview.PreviewMessage;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;

/**
 * Preview data publisher that publishes to the TMS.
 */
public class MessagingPreviewDataPublisher implements PreviewDataPublisher {
  private static final Gson GSON = new Gson();

  private final TopicId topic;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;

  @Inject
  MessagingPreviewDataPublisher(CConfiguration cConf,
                                @Named(PreviewConfigModule.GLOBAL_TMS) MessagingService messagingService) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Preview.MESSAGING_TOPIC));
    this.messagingService = messagingService;
    //TODO using system.metadata properties
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.metadata.");
  }

  @Override
  public void publish(EntityId entityId, PreviewMessage previewMessage) {
    StoreRequest request = StoreRequestBuilder.of(topic).addPayload(GSON.toJson(previewMessage)).build();
    try {
      Retries.callWithRetries(() -> messagingService.publish(request), retryStrategy, Retries.ALWAYS_TRUE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to publish preview message " + previewMessage + " for application " + entityId,
                                 e);
    }
  }
}
