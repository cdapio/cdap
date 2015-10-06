/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link co.cask.cdap.data2.dataset2.DatasetFramework} implementation that talks to DatasetFramework Service
 */
@SuppressWarnings("unchecked")
public class RemoteDatasetFramework extends AbstractDatasetFramework {

  private final LoadingCache<Id.Namespace, DatasetServiceClient> clientCache;

  @Inject
  public RemoteDatasetFramework(final DiscoveryServiceClient discoveryClient,
                                DatasetDefinitionRegistryFactory registryFactory) {
    super(registryFactory);
    this.clientCache = CacheBuilder.newBuilder().build(new CacheLoader<Id.Namespace, DatasetServiceClient>() {
      @Override
      public DatasetServiceClient load(Id.Namespace namespace) throws Exception {
        return new DatasetServiceClient(discoveryClient, namespace);
      }
    });
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module) throws DatasetManagementException {

    // We support easier APIs for custom datasets: user can implement dataset and make it available for others to use
    // by only implementing Dataset. Without requiring implementing datasets module, definition and other classes.
    // In this case we wrap that Dataset implementation with SingleTypeModule. But since we don't have a way to serde
    // dataset modules, if we pass only SingleTypeModule.class the Dataset implementation info will be lost. Hence, as
    // a workaround we put Dataset implementation class in MDS (on DatasetService) and wrapping it with SingleTypeModule
    // when we need to instantiate module.
    //
    // todo: do proper serde for modules instead of just passing class name to server
    Class<?> typeClass;
    if (module instanceof SingleTypeModule) {
      typeClass = ((SingleTypeModule) module).getDataSetClass();
    } else {
      typeClass = module.getClass();
    }

    addModule(moduleId, typeClass, true);
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    clientCache.getUnchecked(moduleId.getNamespace()).deleteModule(moduleId.getId());
  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).deleteModules();
  }

  @Override
  public void addInstance(String datasetType, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .addInstance(datasetInstanceId.getId(), datasetType, props);
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .updateInstance(datasetInstanceId.getId(), props);
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId)
    throws DatasetManagementException {
    return clientCache.getUnchecked(namespaceId).getAllInstances();
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    DatasetMeta meta = clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .getInstance(datasetInstanceId.getId());
    return meta == null ? null : meta.getSpec();
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return clientCache.getUnchecked(datasetInstanceId.getNamespace()).getInstance(datasetInstanceId.getId()) != null;
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return hasType(Id.DatasetType.from(Id.Namespace.SYSTEM, typeName));
  }

  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    return clientCache.getUnchecked(datasetTypeId.getNamespace()).getType(datasetTypeId.getTypeName()) != null;
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getNamespace()).deleteInstance(datasetInstanceId.getId());
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    // delete all one by one
    for (DatasetSpecificationSummary metaSummary : getInstances(namespaceId)) {
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, metaSummary.getName());
      deleteInstance(datasetInstanceId);
    }
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return getAdmin(datasetInstanceId, classLoader, new ConstantClassLoaderProvider(classLoader));
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                             @Nullable ClassLoader parentClassLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    DatasetMeta instanceInfo = clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .getInstance(datasetInstanceId.getId());
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = getDatasetType(instanceInfo.getType(), parentClassLoader, classLoaderProvider);
    return (T) type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), instanceInfo.getSpec());
  }

  @Override
  public <T extends Dataset> T getDataset(
    Id.DatasetInstance datasetInstanceId, Map<String, String> arguments,
    @Nullable ClassLoader classLoader,
    @Nullable Iterable<? extends Id> owners) throws DatasetManagementException, IOException {

    return getDataset(datasetInstanceId, arguments, classLoader, new ConstantClassLoaderProvider(classLoader), owners);
  }

  @Override
  public <T extends Dataset> T getDataset(
    Id.DatasetInstance datasetInstanceId, Map<String, String> arguments,
    @Nullable ClassLoader classLoader) throws DatasetManagementException, IOException {

    return getDataset(datasetInstanceId, arguments, classLoader, null);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(
    Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
    ClassLoader classLoader,
    DatasetClassLoaderProvider classLoaderProvider,
    @Nullable Iterable<? extends Id> owners) throws DatasetManagementException, IOException {

    DatasetMeta instanceInfo = clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .getInstance(datasetInstanceId.getId(), owners);
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = getDatasetType(instanceInfo.getType(), classLoader, classLoaderProvider);
    return (T) type.getDataset(DatasetContext.from(datasetInstanceId.getNamespaceId()),
      instanceInfo.getSpec(), arguments);
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).createNamespace();
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).deleteNamespace();
  }

  @Override
  protected void addModule(Id.DatasetModule module, String typeName, Location jar) throws DatasetManagementException {
    clientCache.getUnchecked(module.getNamespace()).addModule(module.getId(), typeName, jar);
  }
}
