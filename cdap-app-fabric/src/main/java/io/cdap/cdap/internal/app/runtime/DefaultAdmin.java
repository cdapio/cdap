/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.messaging.MessagingAdmin;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.datafabric.dataset.DefaultDatasetManager;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of Admin that delegates dataset operations to a dataset framework.
 */
public class DefaultAdmin extends DefaultDatasetManager implements Admin {

  private final SecureStoreManager secureStoreManager;
  private final MessagingAdmin messagingAdmin;
  private final RetryStrategy retryStrategy;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  /**
   * Creates an instance without messaging admin support.
   */
  public DefaultAdmin(DatasetFramework dsFramework, NamespaceId namespace, SecureStoreManager secureStoreManager,
                      NamespaceQueryAdmin namespaceQueryAdmin) {
    this(dsFramework, namespace, secureStoreManager, null, RetryStrategies.noRetry(), null, namespaceQueryAdmin);
  }

  /**
   * Creates an instance with all Admin functions supported.
   */
  public DefaultAdmin(DatasetFramework dsFramework, NamespaceId namespace,
                      SecureStoreManager secureStoreManager, @Nullable MessagingAdmin messagingAdmin,
                      RetryStrategy retryStrategy, @Nullable KerberosPrincipalId principalId,
                      NamespaceQueryAdmin namespaceQueryAdmin) {
    super(dsFramework, namespace, retryStrategy, principalId);
    this.secureStoreManager = secureStoreManager;
    this.messagingAdmin = messagingAdmin;
    this.retryStrategy = retryStrategy;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public void put(String namespace, String name, String data,
                  @Nullable String description, Map<String, String> properties) throws Exception {
    Retries.runWithRetries(() -> secureStoreManager.put(namespace, name, data, description, properties), retryStrategy);
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    Retries.runWithRetries(() -> secureStoreManager.delete(namespace, name), retryStrategy);
  }

  @Override
  public void createTopic(final String topic) throws TopicAlreadyExistsException, IOException {
    if (messagingAdmin == null) {
      throw new UnsupportedOperationException("Messaging not supported");
    }

    try {
      Retries.runWithRetries(() -> messagingAdmin.createTopic(topic), retryStrategy);
    } catch (TopicAlreadyExistsException | IOException | RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createTopic(final String topic,
                          final Map<String, String> properties) throws TopicAlreadyExistsException, IOException {
    if (messagingAdmin == null) {
      throw new UnsupportedOperationException("Messaging not supported");
    }

    try {
      Retries.runWithRetries(() -> messagingAdmin.createTopic(topic, properties), retryStrategy);
    } catch (TopicAlreadyExistsException | IOException | RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> getTopicProperties(final String topic) throws TopicNotFoundException, IOException {
    if (messagingAdmin == null) {
      throw new UnsupportedOperationException("Messaging not supported");
    }

    try {
      return Retries.callWithRetries(() -> messagingAdmin.getTopicProperties(topic), retryStrategy);
    } catch (TopicNotFoundException | IOException | RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateTopic(final String topic,
                          final Map<String, String> properties) throws TopicNotFoundException, IOException {
    if (messagingAdmin == null) {
      throw new UnsupportedOperationException("Messaging not supported");
    }

    try {
      Retries.runWithRetries(() -> messagingAdmin.updateTopic(topic, properties), retryStrategy);
    } catch (TopicNotFoundException | IOException | RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteTopic(final String topic) throws TopicNotFoundException, IOException {
    if (messagingAdmin == null) {
      throw new UnsupportedOperationException("Messaging not supported");
    }

    try {
      Retries.runWithRetries(() -> messagingAdmin.deleteTopic(topic), retryStrategy);
    } catch (TopicNotFoundException | IOException | RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws IOException {
    try {
      return namespaceQueryAdmin.exists(new NamespaceId(namespace));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Nullable
  @Override
  public NamespaceSummary getNamespaceSummary(String namespace) throws IOException {
    try {
      NamespaceMeta meta = namespaceQueryAdmin.get(new NamespaceId(namespace));
      return new NamespaceSummary(meta.getName(), meta.getDescription(), meta.getGeneration());
    } catch (NamespaceNotFoundException e) {
      return null;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
