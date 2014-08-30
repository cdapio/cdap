/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.data.runtime.DatasetClassLoaderUtil;
import co.cask.cdap.data.runtime.DatasetClassLoaders;
import co.cask.cdap.data2.datafabric.dataset.DatasetAdminWrapper;
import co.cask.cdap.data2.datafabric.dataset.DatasetType;
import co.cask.cdap.data2.datafabric.dataset.DatasetTypeWrapper;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * In-memory implementation of {@link DatasetOpExecutor}.
 */
public class InMemoryDatasetOpExecutor extends AbstractIdleService implements DatasetOpExecutor {

  private final RemoteDatasetFramework client;
  private final LocationFactory locationFactory;

  @Inject
  public InMemoryDatasetOpExecutor(RemoteDatasetFramework client, LocationFactory locationFactory) {
    this.client = client;
    this.locationFactory = locationFactory;
  }

  @Override
  public boolean exists(String instanceName) throws Exception {
    DatasetAdminWrapper datasetAdminWrapper = null;
    try {
      datasetAdminWrapper = getDatasetAdminWrapper(instanceName);
      return datasetAdminWrapper.getDatasetAdmin().exists();
    } finally {
      if (datasetAdminWrapper != null) {
        datasetAdminWrapper.cleanup();
      }
    }
  }

  @Override
  public DatasetSpecification create(String instanceName, DatasetTypeMeta typeMeta, DatasetProperties props)
    throws Exception {
    DatasetTypeWrapper datasetTypeWrapper = null;
    try {
      datasetTypeWrapper = getDatasetTypeWrapper(typeMeta);
      DatasetType type = datasetTypeWrapper.getDatasetType();
      DatasetSpecification spec = type.configure(instanceName, props);
      DatasetAdmin admin = type.getAdmin(spec);
      admin.create();
      return spec;
    } finally {
      if (datasetTypeWrapper != null) {
        datasetTypeWrapper.cleanup();
      }
    }
  }

  @Override
  public void drop(DatasetSpecification spec, DatasetTypeMeta typeMeta) throws Exception {
    DatasetTypeWrapper datasetTypeWrapper = null;
    try {
      datasetTypeWrapper = getDatasetTypeWrapper(typeMeta);
      DatasetAdmin admin = datasetTypeWrapper.getDatasetType().getAdmin(spec);
      admin.drop();
    } finally {
      if (datasetTypeWrapper != null) {
        datasetTypeWrapper.cleanup();
      }
    }
  }

  @Override
  public void truncate(String instanceName) throws Exception {
    DatasetAdminWrapper datasetAdminWrapper = null;
    try {
      datasetAdminWrapper = getDatasetAdminWrapper(instanceName);
      datasetAdminWrapper.getDatasetAdmin().truncate();
    } finally {
      if (datasetAdminWrapper != null) {
        datasetAdminWrapper.cleanup();
      }
    }
  }

  @Override
  public void upgrade(String instanceName) throws Exception {
    DatasetAdminWrapper datasetAdminWrapper = null;
    try {
      datasetAdminWrapper = getDatasetAdminWrapper(instanceName);
      datasetAdminWrapper.getDatasetAdmin().upgrade();
    } finally {
      if (datasetAdminWrapper != null) {
        datasetAdminWrapper.cleanup();
      }
    }
  }

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }

  private DatasetAdminWrapper getDatasetAdminWrapper(String instanceName) throws
    IOException, DatasetManagementException {
    ClassLoader parentClassLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   getClass().getClassLoader());
    Preconditions.checkNotNull(client.getDatasetSpec(instanceName));

    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (parentClassLoader, client.getType(client.getDatasetSpec(instanceName).getType()), locationFactory);

    return new DatasetAdminWrapper(dsUtil, client.getAdmin(instanceName, parentClassLoader));
  }

  private DatasetTypeWrapper getDatasetTypeWrapper(DatasetTypeMeta typeMeta)
    throws Exception {
    ClassLoader parentClassLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                         getClass().getClassLoader());

    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (parentClassLoader, typeMeta, locationFactory);
    DatasetType datasetType = client.getDatasetType(typeMeta, dsUtil.getClassLoader());

    if (datasetType == null) {
      throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
    }
    return new DatasetTypeWrapper(dsUtil, datasetType);
  }


}
