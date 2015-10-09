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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceHandler;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetModuleConflictException;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link DatasetFramework} implementation which performs operations without RPCs.
 * Intended to be used by the dataset service.
 *
 * TODO: ensure namespace exists for each call?
 */
@SuppressWarnings("unchecked")
public class LocalDatasetFramework extends AbstractDatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetFramework.class);

  private final Provider<DatasetInstanceService> instances;
  private final Provider<DatasetTypeManager> types;
  private final Provider<AbstractNamespaceClient> namespaces;

  @Inject
  public LocalDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                               Provider<DatasetInstanceService> instances,
                               Provider<DatasetTypeManager> types,
                               Provider<AbstractNamespaceClient> namespaces) {
    super(registryFactory);
    this.instances = instances;
    this.types = types;
    this.namespaces = namespaces;
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

    addModule(moduleId, typeClass, false);
  }

  @Override
  public void deleteModule(Id.DatasetModule module) throws DatasetManagementException {
    try {
      types.get().deleteModule(module);
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to delete module " + module, e);
    }
  }

  @Override
  public void deleteAllModules(Id.Namespace namespace) throws DatasetManagementException {
    try {
      types.get().deleteModules(namespace);
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to delete all modules in " + namespace, e);
    }
  }

  @Override
  public void addInstance(String datasetType, Id.DatasetInstance instance, DatasetProperties props)
    throws DatasetManagementException {
    try {
      DatasetInstanceConfiguration config = new DatasetInstanceConfiguration(datasetType, props.getProperties());
      instances.get().create(instance.getNamespace(), instance.getId(), config);
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to add instance " + instance + " of type " + datasetType, e);
    }
  }

  @Override
  public void updateInstance(Id.DatasetInstance instance, DatasetProperties props)
    throws DatasetManagementException {

    try {
      instances.get().update(instance, props.getProperties());
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to update instances " + instance, e);
    }
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId)
    throws DatasetManagementException {

    try {
      return DatasetInstanceHandler.spec2Summary(instances.get().list(namespaceId));
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to get instances in namespace " + namespaceId, e);
    }
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance instance) throws DatasetManagementException {
    try {
      DatasetMeta meta = instances.get().get(instance);
      return meta == null ? null : meta.getSpec();
    } catch (NotFoundException e) {
      return null;
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to get dataset spec for instance" + instance, e);
    }
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance instance) throws DatasetManagementException {
    return getDatasetSpec(instance) != null;
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return hasType(Id.DatasetType.from(Id.Namespace.SYSTEM, typeName));
  }

  @Override
  public boolean hasType(Id.DatasetType type) throws DatasetManagementException {
    try {
      return types.get().getTypeInfo(type) != null;
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to get type info for type " + type, e);
    }
  }

  @Override
  public void deleteInstance(Id.DatasetInstance instance) throws DatasetManagementException {
    try {
      instances.get().drop(instance);
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to delete dataset " + instance, e);
    }
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespace) throws DatasetManagementException, IOException {
    // delete all one by one
    for (DatasetSpecificationSummary metaSummary : getInstances(namespace)) {
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, metaSummary.getName());
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

    try {
      DatasetMeta instanceInfo = instances.get().get(datasetInstanceId);
      if (instanceInfo == null) {
        return null;
      }

      DatasetType type = getDatasetType(instanceInfo.getType(), parentClassLoader, classLoaderProvider);
      return (T) type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), instanceInfo.getSpec());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to get dataset admin for instance " + datasetInstanceId, e);
    }
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
    try {
      DatasetMeta meta = instances.get().get(datasetInstanceId, owners);
      if (meta == null) {
        return null;
      }

      DatasetType type = getDatasetType(meta.getType(), classLoader, classLoaderProvider);
      return (T) type.getDataset(DatasetContext.from(datasetInstanceId.getNamespaceId()), meta.getSpec(), arguments);
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to get dataset " + datasetInstanceId, e);
    }
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    try {
      namespaces.get().create(new NamespaceMeta.Builder().setName(namespaceId).build());
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to create namespace " + namespaceId, e);
    }
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    try {
      namespaces.get().delete(namespaceId);
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to create namespace " + namespaceId, e);
    }
  }

  @Override
  protected void addModule(Id.DatasetModule module, String className, Location jar) throws DatasetManagementException {
    try {
      types.get().addModule(module, className, jar);
    } catch (DatasetModuleConflictException e) {
      throw new DatasetManagementException(String.format("Failed to add module %s due to conflict", module), e);
    } catch (Exception e) {
      throw new DatasetManagementException(String.format("Failed to add module %s", module), e);
    }
  }
}
