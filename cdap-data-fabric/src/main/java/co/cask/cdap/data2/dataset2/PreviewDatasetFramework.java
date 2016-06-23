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
package co.cask.cdap.data2.dataset2;


import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Datasetframework that delegates either to local or shared (actual) dataset framework.
 */
public class PreviewDatasetFramework implements DatasetFramework {
  DatasetFramework localDatasetFramework;
  DatasetFramework actualDatasetFramework;

  public PreviewDatasetFramework(DatasetFramework local, DatasetFramework actual) {
    this.localDatasetFramework = local;
    this.actualDatasetFramework = actual;
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module) throws DatasetManagementException {
    if (moduleId.getNamespace().equals(Id.Namespace.SYSTEM)) {
      localDatasetFramework.addModule(moduleId, module);
    }
  }

  @Override
  public void addModule(Id.DatasetModule moduleId,
                        DatasetModule module, Location jarLocation) throws DatasetManagementException {
    if (moduleId.getNamespace().equals(Id.Namespace.SYSTEM)) {
      localDatasetFramework.addModule(moduleId, module, jarLocation);
    }
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    if (moduleId.getNamespace().equals(Id.Namespace.SYSTEM)) {
      localDatasetFramework.deleteModule(moduleId);
    }
  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) throws DatasetManagementException {
    if (Id.Namespace.SYSTEM.equals(namespaceId)) {
      localDatasetFramework.deleteAllModules(namespaceId);
    }
  }

  @Override
  public void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId,
                          DatasetProperties props) throws DatasetManagementException, IOException {
    if (datasetInstanceId.getNamespace().equals(Id.Namespace.SYSTEM)) {
      localDatasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
    }
    // Create the dataset instances corresponding to the Source and Sink during preview
    actualDatasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
  }



  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId,
                             DatasetProperties props) throws DatasetManagementException, IOException {
    if (datasetInstanceId.getNamespace().equals(Id.Namespace.SYSTEM)) {
      localDatasetFramework.updateInstance(datasetInstanceId, props);
    }
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId)
    throws DatasetManagementException {
    if (Id.Namespace.SYSTEM.equals(namespaceId)) {
      return localDatasetFramework.getInstances(namespaceId);
    }
    return actualDatasetFramework.getInstances(namespaceId);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.getDatasetSpec(datasetInstanceId);
    }
    return actualDatasetFramework.getDatasetSpec(datasetInstanceId);
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.hasInstance(datasetInstanceId);
    }
    return actualDatasetFramework.hasInstance(datasetInstanceId);
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return localDatasetFramework.hasSystemType(typeName);
  }

  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    if (Id.Namespace.SYSTEM.equals(datasetTypeId.getNamespace())) {
      return localDatasetFramework.hasType(datasetTypeId);
    }
    return actualDatasetFramework.hasType(datasetTypeId);
  }

  @Override
  public void truncateInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      localDatasetFramework.truncateInstance(datasetInstanceId);
    }
    actualDatasetFramework.truncateInstance(datasetInstanceId);
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      localDatasetFramework.deleteInstance(datasetInstanceId);
    }
    actualDatasetFramework.deleteInstance(datasetInstanceId);
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(namespaceId)) {
      localDatasetFramework.deleteAllInstances(namespaceId);
    }
    actualDatasetFramework.deleteAllInstances(namespaceId);
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                             @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.getAdmin(datasetInstanceId, classLoader);
    }
    return actualDatasetFramework.getAdmin(datasetInstanceId, classLoader);
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                             @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.getAdmin(datasetInstanceId, classLoader, classLoaderProvider);
    }
    return actualDatasetFramework.getAdmin(datasetInstanceId, classLoader, classLoaderProvider);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, owners);
    }
    return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, owners);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader);
    }
    return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners);
    }
    return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments, @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners,
                                          AccessType accessType) throws DatasetManagementException, IOException {
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      return localDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners,
                                              accessType);
    }
    return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners,
                                             accessType);
  }

  @Override
  public void writeLineage(Id.DatasetInstance datasetInstanceId, AccessType accessType) {
    //TODO: verify this
    if (Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace())) {
      localDatasetFramework.writeLineage(datasetInstanceId, accessType);
    }
    actualDatasetFramework.writeLineage(datasetInstanceId, accessType);
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    throw new UnsupportedOperationException(String.format("Namespace %s cannot be created in preview", namespaceId));
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    throw new UnsupportedOperationException(String.format("Namespace %s cannot be deleted in preview", namespaceId));
  }
}
