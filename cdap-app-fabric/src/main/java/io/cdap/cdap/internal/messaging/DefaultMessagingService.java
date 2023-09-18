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

package io.cdap.cdap.internal.messaging;

import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

public class DefaultMessagingService implements MessagingService {

  private MessagingService delegate;

  private final CConfiguration cConf;
  private final Injector injector;

  private final MessagingServiceExtensionLoader extensionLoader;

  private final String name;

  @Inject
  public DefaultMessagingService(
      CConfiguration cConf, Injector injector, MessagingServiceExtensionLoader extensionLoader) {
    this.injector = injector;
    this.cConf = cConf;
    this.extensionLoader = extensionLoader;

    // todo change this after tests
    this.name = "SYSTEM";
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
  public TopicMetadata getTopic(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    return getDelegate().getTopic(topicId);
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId)
      throws IOException, UnauthorizedException {
    return getDelegate().listTopics(namespaceId);
  }

  @Override
  public MessageFetcher prepareFetch(TopicId topicId) throws TopicNotFoundException, IOException {
    return getDelegate().prepareFetch(topicId);
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    return getDelegate().publish(request);
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
      messagingService = extensionLoader.get(this.name);

      if (messagingService == null) {
        throw new IllegalArgumentException("Unsupported storage implementation " + name);
      }

      this.delegate = messagingService;
      return messagingService;
    }
  }
}
