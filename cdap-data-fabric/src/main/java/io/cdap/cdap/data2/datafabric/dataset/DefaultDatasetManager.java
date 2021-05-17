/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetManager;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.InstanceNotFoundException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link DatasetManager} that performs operation via {@link DatasetFramework}.
 */
public class DefaultDatasetManager implements DatasetManager {

  private final DatasetFramework datasetFramework;
  private final NamespaceId namespaceId;
  private final RetryStrategy retryStrategy;
  @Nullable
  private final KerberosPrincipalId principalId;

  /**
   * Constructor.
   * @param datasetFramework the {@link DatasetFramework} to use for performing the actual operation
   * @param namespaceId the {@link NamespaceId} for all dataset managed through this class
   * @param retryStrategy the {@link RetryStrategy} to use for {@link RetryableException}.
   * @param principalId the {@link KerberosPrincipalId} for all datasets created.
   */
  public DefaultDatasetManager(DatasetFramework datasetFramework,
                               NamespaceId namespaceId,
                               RetryStrategy retryStrategy,
                               @Nullable KerberosPrincipalId principalId) {
    this.datasetFramework = datasetFramework;
    this.namespaceId = namespaceId;
    this.retryStrategy = retryStrategy;
    this.principalId = principalId;
  }

  @Override
  public boolean datasetExists(final String name) throws DatasetManagementException, UnauthorizedException {
    try {
      return Retries.callWithRetries(new Retries.Callable<Boolean, Exception>() {
        @Override
        public Boolean call() throws DatasetManagementException, UnauthorizedException {
          return datasetFramework.getDatasetSpec(createInstanceId(name)) != null;
        }
      }, retryStrategy);
    } catch (UnauthorizedException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String getDatasetType(final String name) throws DatasetManagementException, UnauthorizedException {
    try {
      return Retries.callWithRetries(new Retries.Callable<String, Exception>() {
        @Override
        public String call() throws DatasetManagementException, UnauthorizedException {
          DatasetSpecification spec = datasetFramework.getDatasetSpec(createInstanceId(name));
          if (spec == null) {
            throw new InstanceNotFoundException(name);
          }
          return spec.getType();
        }
      }, retryStrategy);
    } catch (UnauthorizedException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public DatasetProperties getDatasetProperties(final String name)
    throws DatasetManagementException, UnauthorizedException {
    try {
      return Retries.callWithRetries(new Retries.Callable<DatasetProperties, Exception>() {
        @Override
        public DatasetProperties call() throws DatasetManagementException, UnauthorizedException {
          DatasetSpecification spec = datasetFramework.getDatasetSpec(createInstanceId(name));
          if (spec == null) {
            throw new InstanceNotFoundException(name);
          }
          return DatasetProperties.of(spec.getOriginalProperties());
        }
      }, retryStrategy);
    } catch (UnauthorizedException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void createDataset(final String name, final String type,
                            final DatasetProperties properties)
    throws DatasetManagementException, UnauthorizedException {
    try {
      Retries.runWithRetries(() -> {
        // we have to do this check since addInstance method can only be used when app impersonation is enabled
        if (principalId != null) {
          datasetFramework.addInstance(type, createInstanceId(name), properties, principalId);
        } else {
          datasetFramework.addInstance(type, createInstanceId(name), properties);
        }
      }, retryStrategy);
    } catch (IOException ioe) {
      // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
      throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                         name, ioe.getMessage()), ioe);
    } catch (UnauthorizedException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void updateDataset(final String name, final DatasetProperties properties)
    throws DatasetManagementException, UnauthorizedException {
    try {
      Retries.runWithRetries(() -> {
          datasetFramework.updateInstance(createInstanceId(name), properties);
      }, retryStrategy);
    } catch (IOException ioe) {
      // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
      throw new DatasetManagementException(String.format("Failed to update instance %s, details: %s",
                                                         name, ioe.getMessage()), ioe);
    } catch (UnauthorizedException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void dropDataset(final String name) throws DatasetManagementException, UnauthorizedException {
    try {
      Retries.runWithRetries(() -> {
          datasetFramework.deleteInstance(createInstanceId(name));
      }, retryStrategy);
    } catch (IOException ioe) {
      // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
      throw new DatasetManagementException(String.format("Failed to delete instance %s, details: %s",
                                                         name, ioe.getMessage()), ioe);
    } catch (UnauthorizedException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void truncateDataset(final String name) throws DatasetManagementException, UnauthorizedException {
    try {
      Retries.runWithRetries(() -> {
          datasetFramework.truncateInstance(createInstanceId(name));
      }, retryStrategy);
    } catch (IOException ioe) {
      // not the prettiest message, but this replicates exactly what RemoteDatasetFramework throws
      throw new DatasetManagementException(String.format("Failed to truncate instance %s, details: %s",
                                                         name, ioe.getMessage()), ioe);
    } catch (UnauthorizedException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private DatasetId createInstanceId(String name) {
    return namespaceId.dataset(name);
  }
}
