/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.Updatable;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.data2.datafabric.dataset.DatasetType;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;

/**
 * In-memory implementation of {@link DatasetOpExecutor}.
 */
public class InMemoryDatasetOpExecutor implements DatasetOpExecutor {

  private final RemoteDatasetFramework client;

  @Inject
  public InMemoryDatasetOpExecutor(RemoteDatasetFramework client) {
    this.client = client;
  }

  @Override
  public boolean exists(DatasetId datasetInstanceId) throws Exception {
    return getAdmin(datasetInstanceId).exists();
  }

  @Override
  public DatasetCreationResponse create(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                                        DatasetProperties props)
    throws Exception {

    DatasetType type = client.getDatasetType(typeMeta, null, new ConstantClassLoaderProvider());

    if (type == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }

    DatasetSpecification spec = type.configure(datasetInstanceId.getEntityName(), props);
    DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespace()), spec);
    admin.create();

    return new DatasetCreationResponse(spec, null);
  }

  @Override
  public DatasetCreationResponse update(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                                        DatasetProperties props, DatasetSpecification existing) throws Exception {
    DatasetType type = client.getDatasetType(typeMeta, null, new ConstantClassLoaderProvider());
    if (type == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }
    try {
      DatasetSpecification spec = type.reconfigure(datasetInstanceId.getEntityName(), props, existing);
      DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespace()), spec);
      if (admin instanceof Updatable) {
        ((Updatable) admin).update(existing);
      } else {
        admin.create();
      }
      if (spec.getDescription() == null && existing.getDescription() != null) {
        spec.setDescription(existing.getDescription());
      }
      return new DatasetCreationResponse(spec, null);
    } catch (IncompatibleUpdateException e) {
      throw new ConflictException(e.getMessage());
    }
  }

  @Override
  public void drop(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                   DatasetSpecification spec) throws Exception {
    DatasetType type = client.getDatasetType(typeMeta, null, new ConstantClassLoaderProvider());

    if (type == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }

    DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespace()), spec);
    admin.drop();
  }

  @Override
  public void truncate(DatasetId datasetInstanceId) throws Exception {
    getAdmin(datasetInstanceId).truncate();
  }

  @Override
  public void upgrade(DatasetId datasetInstanceId) throws Exception {
    getAdmin(datasetInstanceId).upgrade();
  }

  private DatasetAdmin getAdmin(DatasetId datasetInstanceId)
    throws IOException, DatasetManagementException, UnauthorizedException {
    return client.getAdmin(datasetInstanceId, null);
  }
}
