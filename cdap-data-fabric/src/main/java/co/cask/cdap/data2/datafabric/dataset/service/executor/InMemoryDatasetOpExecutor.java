/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.Updatable;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.data2.datafabric.dataset.DatasetType;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetId;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * In-memory implementation of {@link DatasetOpExecutor}.
 */
public class InMemoryDatasetOpExecutor extends AbstractIdleService implements DatasetOpExecutor {

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
  public DatasetSpecification create(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props)
    throws Exception {

    DatasetType type = client.getDatasetType(typeMeta, null, new ConstantClassLoaderProvider());

    if (type == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }

    DatasetSpecification spec = type.configure(datasetInstanceId.getEntityName(), props);
    DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespace()), spec);
    admin.create();

    return spec;
  }

  @Override
  public DatasetSpecification update(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
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
      return spec;
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

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }

  private DatasetAdmin getAdmin(DatasetId datasetInstanceId) throws IOException, DatasetManagementException {
    return client.getAdmin(datasetInstanceId, null);
  }
}
