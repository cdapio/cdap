/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;

/**
 * Default implementation of {@link DatasetManager} that performs operation via {@link DatasetFramework}.
 */
public class DefaultDatasetManager implements DatasetManager {

  private final DatasetFramework datasetFramework;
  private final NamespaceId namespaceId;
  private final RetryStrategy retryStrategy;

  /**
   * Constructor.
   *
   * @param datasetFramework the {@link DatasetFramework} to use for performing the actual operation
   * @param namespaceId the {@link NamespaceId} for all dataset managed through this class
   * @param retryStrategy the {@link RetryStrategy} to use for {@link RetryableException}.
   */
  public DefaultDatasetManager(DatasetFramework datasetFramework,
                               NamespaceId namespaceId,
                               RetryStrategy retryStrategy) {
    this.datasetFramework = datasetFramework;
    this.namespaceId = namespaceId;
    this.retryStrategy = retryStrategy;
  }

  @Override
  public boolean datasetExists(final String name) throws DatasetManagementException {
    return Retries.callWithRetries(new Retries.Callable<Boolean, DatasetManagementException>() {
      @Override
      public Boolean call() throws DatasetManagementException {
        return datasetFramework.getDatasetSpec(createInstanceId(name)) != null;
      }
    }, retryStrategy);
  }

  @Override
  public String getDatasetType(final String name) throws DatasetManagementException {
    return Retries.callWithRetries(new Retries.Callable<String, DatasetManagementException>() {
      @Override
      public String call() throws DatasetManagementException {
        DatasetSpecification spec = datasetFramework.getDatasetSpec(createInstanceId(name));
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
        DatasetSpecification spec = datasetFramework.getDatasetSpec(createInstanceId(name));
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
          datasetFramework.addInstance(type, createInstanceId(name), properties);
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
          datasetFramework.updateInstance(createInstanceId(name), properties);
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
          datasetFramework.deleteInstance(createInstanceId(name));
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
          datasetFramework.truncateInstance(createInstanceId(name));
        } catch (IOException ioe) {
          // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
          throw new DatasetManagementException(String.format("Failed to truncate instance %s, details: %s",
                                                             name, ioe.getMessage()), ioe);
        }
        return null;
      }
    }, retryStrategy);
  }

  private DatasetId createInstanceId(String name) {
    return namespaceId.dataset(name);
  }
}
