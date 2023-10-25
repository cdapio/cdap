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

package io.cdap.cdap.messaging.client;

import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.MessagingSystem;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.messaging.spi.RollbackDetail;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delegates {@link io.cdap.cdap.messaging.spi.MessagingService} based on configured extension */
public class DelegatingMessagingService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(DelegatingMessagingService.class);
  private MessagingService delegate;
  private final CConfiguration cConf;
  private final MessagingServiceExtensionLoader extensionLoader;

  @Inject
  public DelegatingMessagingService(
      CConfiguration cConf, MessagingServiceExtensionLoader extensionLoader) {
    this.cConf = cConf;
    this.extensionLoader = extensionLoader;
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    getDelegate().createTopic(topicMetadata);
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    getDelegate().updateTopic(topicMetadata);
  }

  @Override
  public void deleteTopic(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    getDelegate().deleteTopic(topicId);
  }

  @Override
  public String getName() {
    return cConf.get(MessagingSystem.MESSAGING_SERVICE_NAME);
  }

  @Override
  public Map<String, String> getTopicMetadataProperties(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    return getDelegate().getTopicMetadataProperties(topicId);
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    return getDelegate().publish(request);
  }

  @Override
  public CloseableIterator<RawMessage> fetch(MessageFetchRequest messageFetchRequest)
      throws TopicNotFoundException, IOException {
    return getDelegate().fetch(messageFetchRequest);
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId)
      throws IOException, UnauthorizedException {
    return getDelegate().listTopics(namespaceId);
  }

  @Override
  public void storePayload(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    getDelegate().storePayload(request);
  }

  @Override
  public void rollback(TopicId topicId, RollbackDetail rollbackDetail)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    getDelegate().rollback(topicId, rollbackDetail);
  }

  private MessagingService getDelegate() {
    MessagingService messagingService = this.delegate;
    if (messagingService != null) {
      return messagingService;
    }
    synchronized (this) {
      messagingService = this.delegate;
      if (messagingService != null) {
        return messagingService;
      }
      messagingService = extensionLoader.get(getName());

      if (messagingService == null) {
        throw new IllegalArgumentException(
            "Unsupported messaging service implementation " + getName());
      }
      LOG.info("Messaging service {} is loaded", messagingService.getName());

      this.delegate = messagingService;
      return messagingService;
    }
  }
}
