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
package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A simple implementation of {@link co.cask.cdap.data2.dataset2.DatasetFramework} that keeps its state in
 * memory and also delegates to Remote datasetFramework.
 */
public class HybridDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDatasetFramework.class);

  private final DatasetFramework inMemoryDatasetFramework;
  private final DatasetFramework remoteDatasetFramework;

  public HybridDatasetFramework(DatasetFramework inMemory, DatasetFramework remote) {
    this.inMemoryDatasetFramework = inMemory;
    this.remoteDatasetFramework = remote;
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module) throws DatasetManagementException {
    // no-op, we assume inMemory and remote frameworks are configured already.
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module,
                        Location jarLocation) throws DatasetManagementException {
    // no-op, we assume inMemory and remote frameworks are configured already.
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {

  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) throws DatasetManagementException {

  }

  @Override
  public void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId,
                          DatasetProperties props) throws DatasetManagementException, IOException {
    if (remoteDatasetFramework.hasInstance(datasetInstanceId) ||
      inMemoryDatasetFramework.hasInstance(datasetInstanceId)) {
      return;
    } else {
     inMemoryDatasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
    }
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId,
                             DatasetProperties props) throws DatasetManagementException, IOException {

  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId)
    throws DatasetManagementException {
    return remoteDatasetFramework.getInstances(namespaceId);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return null;
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    if (inMemoryDatasetFramework.hasInstance(datasetInstanceId)) {
      return true;
    } else {
      return remoteDatasetFramework.hasInstance(datasetInstanceId);
    }
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return false;
  }

  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    return false;
  }

  @Override
  public void truncateInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {

  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {

  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {

  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                             @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return null;
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                             @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    return null;
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String,
    String> arguments, @Nullable ClassLoader classLoader, @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    if (inMemoryDatasetFramework.hasInstance(datasetInstanceId)) {
      return inMemoryDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, owners);
    } else {
      return remoteDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, owners);
    }
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    if (datasetInstanceId.getNamespace().equals(Id.Namespace.SYSTEM)) {
      return inMemoryDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader);
    } else {
      return remoteDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader);
    }
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    return getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners, null);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners, AccessType accessType)
    throws DatasetManagementException, IOException {
    if (inMemoryDatasetFramework.hasInstance(datasetInstanceId)) {
      return inMemoryDatasetFramework.getDataset(
        datasetInstanceId, arguments, classLoader, classLoaderProvider, owners, accessType);
    } else {
      return remoteDatasetFramework.getDataset(
        datasetInstanceId, arguments, classLoader, classLoaderProvider, owners, accessType);
    }
  }

  @Override
  public void writeLineage(Id.DatasetInstance datasetInstanceId, AccessType accessType) {

  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {

  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {

  }
}
