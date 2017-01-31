/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link DatasetFramework} which forwards all calls to the underlying
 * delegate.
 */
public class ForwardingDatasetFramework implements DatasetFramework {

  protected final DatasetFramework delegate;

  public ForwardingDatasetFramework(DatasetFramework datasetFramework) {
    this.delegate = datasetFramework;
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module) throws DatasetManagementException {
    delegate.addModule(moduleId, module);
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module, Location jarLocation)
    throws DatasetManagementException {
    delegate.addModule(moduleId, module, jarLocation);
  }

  @Override
  public void deleteModule(DatasetModuleId moduleId) throws DatasetManagementException {
    delegate.deleteModule(moduleId);
  }

  @Override
  public void deleteAllModules(NamespaceId namespaceId) throws DatasetManagementException {
    delegate.deleteAllModules(namespaceId);
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    delegate.addInstance(datasetTypeName, datasetInstanceId, props);
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId, DatasetProperties props,
                          @Nullable KerberosPrincipalId ownerPrincipal)
    throws DatasetManagementException, IOException {
    delegate.addInstance(datasetTypeName, datasetInstanceId, props, ownerPrincipal);
  }

  @Override
  public void updateInstance(DatasetId datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    delegate.updateInstance(datasetInstanceId, props);
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(NamespaceId namespaceId)
    throws DatasetManagementException {
    return delegate.getInstances(namespaceId);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(DatasetId datasetInstanceId) throws DatasetManagementException {
    return delegate.getDatasetSpec(datasetInstanceId);
  }

  @Override
  public boolean hasInstance(DatasetId datasetInstanceId) throws DatasetManagementException {
    return delegate.hasInstance(datasetInstanceId);
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return delegate.hasSystemType(typeName);
  }

  @Override
  public boolean hasType(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return delegate.hasType(datasetTypeId);
  }

  @Nullable
  @Override
  public DatasetTypeMeta getTypeInfo(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return delegate.getTypeInfo(datasetTypeId);
  }

  @Override
  public void truncateInstance(DatasetId datasetInstanceId) throws DatasetManagementException, IOException {
    delegate.truncateInstance(datasetInstanceId);
  }

  @Override
  public void deleteInstance(DatasetId datasetInstanceId) throws DatasetManagementException, IOException {
    delegate.deleteInstance(datasetInstanceId);
  }

  @Override
  public void deleteAllInstances(NamespaceId namespaceId) throws DatasetManagementException, IOException {
    delegate.deleteAllInstances(namespaceId);
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return delegate.getAdmin(datasetInstanceId, classLoader);
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    return delegate.getAdmin(datasetInstanceId, classLoader, classLoaderProvider);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return delegate.getDataset(datasetInstanceId, arguments, classLoader);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends EntityId> owners, AccessType accessType)
    throws DatasetManagementException, IOException {
    return delegate.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners, accessType);
  }

  @Override
  public void writeLineage(DatasetId datasetInstanceId, AccessType accessType) {
    delegate.writeLineage(datasetInstanceId, accessType);
  }
}
