/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.data2.datafabric.dataset.DatasetType;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
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
  public boolean exists(Id.DatasetInstance datasetInstanceId) throws Exception {
    return getAdmin(datasetInstanceId).exists();
  }

  @Override
  public DatasetSpecification create(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props)
    throws Exception {

    DatasetType type = client.getDatasetType(typeMeta, new ConstantClassLoaderProvider());

    if (type == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }

    // TODO: Note - for now, just sending the name. However, type likely needs to be namesapce-aware too.
    DatasetSpecification spec = type.configure(datasetInstanceId.getId(), props);
    DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec);
    admin.create();

    return spec;
  }

  @Override
  public void drop(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta,
                   DatasetSpecification spec) throws Exception {
    DatasetType type = client.getDatasetType(typeMeta, new ConstantClassLoaderProvider());

    if (type == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }

    DatasetAdmin admin = type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec);
    admin.drop();
  }

  @Override
  public void truncate(Id.DatasetInstance datasetInstanceId) throws Exception {
    getAdmin(datasetInstanceId).truncate();
  }

  @Override
  public void upgrade(Id.DatasetInstance datasetInstanceId) throws Exception {
    getAdmin(datasetInstanceId).upgrade();
  }

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }

  private DatasetAdmin getAdmin(Id.DatasetInstance datasetInstanceId) throws IOException, DatasetManagementException {
    return client.getAdmin(datasetInstanceId, null);
  }
}
