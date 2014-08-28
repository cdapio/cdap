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
import co.cask.cdap.data2.datafabric.dataset.DatasetType;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    DatasetClassLoaderUtil dsUtil = null;
    try {
      ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                     getClass().getClassLoader());
      dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
        (classLoader, client.getType(client.getDatasetSpec(instanceName).getType()), locationFactory);
      boolean exists = getAdmin(instanceName, dsUtil.getClassLoader()).exists();
      return exists;
    } finally {
      if (dsUtil != null) {
        dsUtil.cleanup();
      }
    }
  }

  @Override
  public DatasetSpecification create(String instanceName, DatasetTypeMeta typeMeta, DatasetProperties props)
    throws Exception {
    DatasetClassLoaderUtil dsUtil = null;
    try {
      ClassLoader cl = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                            getClass().getClassLoader());
      dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType(cl, typeMeta, locationFactory);
      DatasetType type = client.getDatasetType(typeMeta, dsUtil.getClassLoader());

      if (type == null) {
        throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
      }
      DatasetSpecification spec = type.configure(instanceName, props);
      DatasetAdmin admin = type.getAdmin(spec);
      admin.create();
      return spec;
    } finally {
      //todo: will this affect the spec used by the caller ? , if so, where should we cleanup
      if (dsUtil != null) {
        dsUtil.cleanup();
      }
    }
  }

  @Override
  public void drop(DatasetSpecification spec, DatasetTypeMeta typeMeta) throws Exception {
    DatasetClassLoaderUtil dsUtil = null;
    try {
      ClassLoader cl = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                            getClass().getClassLoader());
      dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType(cl, typeMeta, locationFactory);
      DatasetType type = client.getDatasetType(typeMeta, dsUtil.getClassLoader());

      if (type == null) {
        throw new IllegalArgumentException("Dataset type cannot be instantiated for provided type meta: " + typeMeta);
      }

      DatasetAdmin admin = type.getAdmin(spec);
      admin.drop();
    } finally {
      if (dsUtil != null) {
        dsUtil.cleanup();
      }
    }
  }

  @Override
  public void truncate(String instanceName) throws Exception {
    DatasetClassLoaderUtil dsUtil = null;
    try {
      ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                     getClass().getClassLoader());
      dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
        (classLoader, client.getType(client.getDatasetSpec(instanceName).getType()), locationFactory);
      getAdmin(instanceName, dsUtil.getClassLoader()).truncate();
    } finally {
      if (dsUtil != null) {
        dsUtil.cleanup();
      }
    }
  }

  @Override
  public void upgrade(String instanceName) throws Exception {
    ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   getClass().getClassLoader());
    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (classLoader, client.getType(client.getDatasetSpec(instanceName).getType()), locationFactory);
    getAdmin(instanceName, dsUtil.getClassLoader()).upgrade();
    dsUtil.cleanup();
  }

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }

  private DatasetAdmin getAdmin(String instanceName, ClassLoader classLoader) throws
    IOException, DatasetManagementException {
    return client.getAdmin(instanceName, classLoader);
  }

}
