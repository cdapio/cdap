/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.messaging.MessagingAdmin;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of Admin that delegates dataset operations to a dataset framework.
 */
public class DefaultAdmin implements Admin {

  private final DatasetFramework dsFramework;
  private final NamespaceId namespace;
  private final SecureStoreManager secureStoreManager;
  private final MessagingAdmin messagingAdmin;
  private final RetryStrategy retryStrategy;

  /**
   * Creates an instance without messaging admin support.
   */
  public DefaultAdmin(DatasetFramework dsFramework, NamespaceId namespace, SecureStoreManager secureStoreManager) {
    this(dsFramework, namespace, secureStoreManager, null, RetryStrategies.noRetry());
  }

  /**
   * Creates an instance with all Admin functions supported.
   */
  public DefaultAdmin(DatasetFramework dsFramework, NamespaceId namespace,
                      SecureStoreManager secureStoreManager, @Nullable MessagingAdmin messagingAdmin,
                      RetryStrategy retryStrategy) {
    this.dsFramework = dsFramework;
    this.namespace = namespace;
    this.secureStoreManager = secureStoreManager;
    this.messagingAdmin = messagingAdmin;
    this.retryStrategy = retryStrategy;
  }

  private DatasetId createInstanceId(String name) {
    return namespace.dataset(name);
  }

  @Override
  public boolean datasetExists(final String name) throws DatasetManagementException {
    return Retries.callWithRetries(new Retries.Callable<Boolean, DatasetManagementException>() {
      @Override
      public Boolean call() throws DatasetManagementException {
        return dsFramework.getDatasetSpec(createInstanceId(name)) != null;
      }
    }, retryStrategy);
  }

  @Override
  public String getDatasetType(final String name) throws DatasetManagementException {
    return Retries.callWithRetries(new Retries.Callable<String, DatasetManagementException>() {
      @Override
      public String call() throws DatasetManagementException {
        DatasetSpecification spec = dsFramework.getDatasetSpec(createInstanceId(name));
        if (spec == null) {
          throw new InstanceNotFoundException(name);
        }
        return spec.getType();
      }
    }, retryStrategy);
  }

  @Override
  public DatasetProperties getDatasetProperties(final String name) throws DatasetManagementException {
    return Retries.callWithRetries(new Retries.Callable<DatasetProperties, DatasetManagementException>() {
      @Override
      public DatasetProperties call() throws DatasetManagementException {
        DatasetSpecification spec = dsFramework.getDatasetSpec(createInstanceId(name));
        if (spec == null) {
          throw new InstanceNotFoundException(name);
        }
        return DatasetProperties.of(spec.getOriginalProperties());
      }
    }, retryStrategy);
  }

  @Override
  public void createDataset(final String name, final String type,
                            final DatasetProperties properties) throws DatasetManagementException {
    Retries.callWithRetries(new Retries.Callable<Void, DatasetManagementException>() {
      @Override
      public Void call() throws DatasetManagementException {
        try {
          dsFramework.addInstance(type, createInstanceId(name), properties);
        } catch (IOException ioe) {
          // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
          throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                             name, ioe.getMessage()), ioe);
        }
        return null;
      }
    }, retryStrategy);
  }

  @Override
  public void updateDataset(final String name, final DatasetProperties properties) throws DatasetManagementException {
    Retries.callWithRetries(new Retries.Callable<Void, DatasetManagementException>() {
      @Override
      public Void call() throws DatasetManagementException {
        try {
          dsFramework.updateInstance(createInstanceId(name), properties);
        } catch (IOException ioe) {
          // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
          throw new DatasetManagementException(String.format("Failed to update instance %s, details: %s",
                                                             name, ioe.getMessage()), ioe);
        }
        return null;
      }
    }, retryStrategy);
  }

  @Override
  public void dropDataset(final String name) throws DatasetManagementException {
    Retries.callWithRetries(new Retries.Callable<Void, DatasetManagementException>() {
      @Override
      public Void call() throws DatasetManagementException {
        try {
          dsFramework.deleteInstance(createInstanceId(name));
        } catch (IOException ioe) {
          // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
          throw new DatasetManagementException(String.format("Failed to delete instance %s, details: %s",
                                                             name, ioe.getMessage()), ioe);
        }
        return null;
      }
    }, retryStrategy);
  }

  @Override
  public void truncateDataset(final String name) throws DatasetManagementException {
    Retries.callWithRetries(new Retries.Callable<Void, DatasetManagementException>() {
      @Override
      public Void call() throws DatasetManagementException {
        try {
          dsFramework.truncateInstance(createInstanceId(name));
        } catch (IOException ioe) {
          // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
          throw new DatasetManagementException(String.format("Failed to truncate instance %s, details: %s",
                                                             name, ioe.getMessage()), ioe);
        }
        return null;
      }
    }, retryStrategy);
  }

  @Override
  public void putSecureData(String namespace, String name, String data,
                            String description, Map<String, String> properties) throws Exception {
    secureStoreManager.putSecureData(namespace, name, data, description, properties);
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    secureStoreManager.deleteSecureData(namespace, name);
  }

  @Override
  public void createTopic(final String topic) throws TopicAlreadyExistsException, IOException {
    if (messagingAdmin == null) {
      throw new UnsupportedOperationException("Messaging not supported");
    }

    try {
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws TopicAlreadyExistsException, IOException {
          messagingAdmin.createTopic(topic);
          return null;
        }
      }, retryStrategy);
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
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws TopicAlreadyExistsException, IOException {
          messagingAdmin.createTopic(topic, properties);
          return null;
        }
      }, retryStrategy);
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
      return Retries.callWithRetries(new Retries.Callable<Map<String, String>, Exception>() {
        @Override
        public Map<String, String> call() throws TopicNotFoundException, IOException {
          return messagingAdmin.getTopicProperties(topic);
        }
      }, retryStrategy);
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
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws TopicNotFoundException, IOException {
          messagingAdmin.updateTopic(topic, properties);
          return null;
        }
      }, retryStrategy);
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
      Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
        @Override
        public Void call() throws TopicNotFoundException, IOException {
          messagingAdmin.deleteTopic(topic);
          return null;
        }
      }, retryStrategy);
    } catch (TopicNotFoundException | IOException | RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

}
