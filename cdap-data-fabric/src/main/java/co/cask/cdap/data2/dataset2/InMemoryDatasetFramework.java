/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

/**
 * A simple implementation of {@link co.cask.cdap.data2.dataset2.DatasetFramework} that keeps its state in
 * memory
 */
@SuppressWarnings("unchecked")
public class InMemoryDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDatasetFramework.class);

  private final DatasetDefinitionRegistryFactory registryFactory;
  private final Set<Id.Namespace> namespaces;
  private final SetMultimap<Id.Namespace, String> nonDefaultTypes;

  // Id.Namespace is contained in Id.DatasetInstance. But we need to be able to get all instances in a namespace
  // and delete all instances in a namespace, so we keep it as a separate key
  private final Table<Id.Namespace, Id.DatasetInstance, DatasetSpecification> instances;
  private final Table<Id.Namespace, Id.DatasetModule, String> moduleClasses;

  private final Lock readLock;
  private final Lock writeLock;

  private final boolean allowDatasetUncheckedUpgrade;

  // NOTE: used only for "internal" operations, that doesn't return to client object of custom type
  // NOTE: for getting dataset/admin objects we construct fresh new one using all modules (no dependency management in
  //       this in-mem implementation for now) and passed client (program) class loader
  // NOTE: We maintain one DatasetDefinitionRegistry per namespace
  private final Map<Id.Namespace, DatasetDefinitionRegistry> registries;

  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory, CConfiguration configuration) {
    this(registryFactory, new HashMap<String, DatasetModule>(), configuration);
  }

  @Inject
  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                                  @Named("defaultDatasetModules") Map<String, DatasetModule> defaultModules,
                                  CConfiguration configuration) {
    this.registryFactory = registryFactory;
    this.allowDatasetUncheckedUpgrade = configuration.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);

    this.namespaces = Sets.newHashSet();
    this.nonDefaultTypes = HashMultimap.create();
    this.instances = HashBasedTable.create();
    this.registries = Maps.newHashMap();
    // the order in which module classes are inserted is important,
    // so we use a table where Map<Id.DatasetModule, String> is a LinkedHashMap
    Map<Id.Namespace, Map<Id.DatasetModule, String>> backingMap = Maps.newHashMap();
    this.moduleClasses = Tables.newCustomTable(backingMap, new Supplier<Map<Id.DatasetModule, String>>() {
      @Override
      public Map<Id.DatasetModule, String> get() {
        return Maps.newLinkedHashMap();
      }
    });

    // add default dataset modules to system namespace
    namespaces.add(Constants.SYSTEM_NAMESPACE_ID);
    DatasetDefinitionRegistry systemRegistry = registryFactory.create();
    for (Map.Entry<String, DatasetModule> entry : defaultModules.entrySet()) {
      LOG.info("Adding Default module {} to system namespace", entry.getKey());
      String moduleName = entry.getKey();
      DatasetModule module = entry.getValue();
      entry.getValue().register(systemRegistry);
      // keep track of default module classes. These are used when creating registries for other namespaces,
      // which need to register system classes too.
      String moduleClassName = DatasetModules.getDatasetModuleClass(module).getName();
      Id.DatasetModule moduleId = Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE_ID, moduleName);
      moduleClasses.put(Constants.SYSTEM_NAMESPACE_ID, moduleId, moduleClassName);
    }
    registries.put(Constants.SYSTEM_NAMESPACE_ID, systemRegistry);

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }

  @Override
  public void addModule(Id.DatasetModule moduleId,
                        DatasetModule module) throws ModuleConflictException {
    writeLock.lock();
    try {
      if (moduleClasses.contains(moduleId.getNamespace(), moduleId)) {
        throw new ModuleConflictException(String.format("Cannot add module '%s', it already exists.", moduleId));
      }

      DatasetDefinitionRegistry registry = registries.get(moduleId.getNamespace());
      if (registry == null) {
        registry = registryFactory.create();
        registries.put(moduleId.getNamespace(), registry);
      }
      TypesTrackingRegistry trackingRegistry = new TypesTrackingRegistry(registry);
      module.register(trackingRegistry);
      String moduleClassName = DatasetModules.getDatasetModuleClass(module).getName();
      moduleClasses.put(moduleId.getNamespace(), moduleId, moduleClassName);
      nonDefaultTypes.putAll(moduleId.getNamespace(), trackingRegistry.getTypes());
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) {
    // todo: check if existing datasets or modules use this module
    writeLock.lock();
    try {
      moduleClasses.remove(moduleId.getNamespace(), moduleId);
      LinkedHashSet<String> availableModuleClasses = getAvailableModuleClasses(moduleId.getNamespace());
      // this will cleanup types
      DatasetDefinitionRegistry registry = createRegistry(availableModuleClasses,
                                                          registries.getClass().getClassLoader());
      registries.put(moduleId.getNamespace(), registry);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) throws ModuleConflictException {
    writeLock.lock();
    try {
      // check if there are any datasets that use types from the namespace from which we want to remove all modules
      Set<String> typesInNamespace = nonDefaultTypes.get(namespaceId);
      for (DatasetSpecification spec : instances.row(namespaceId).values()) {
        if (typesInNamespace.contains(spec.getType())) {
          throw new ModuleConflictException(
            String.format("Cannot delete all modules in namespace '%s', some datasets use them", namespaceId));
        }
      }
      moduleClasses.row(namespaceId).clear();
      nonDefaultTypes.removeAll(namespaceId);
      registries.put(namespaceId, registryFactory.create());
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addInstance(String datasetType, Id.DatasetInstance datasetInstanceId,
                                       DatasetProperties props) throws DatasetManagementException, IOException {
    writeLock.lock();
    try {
      if (!allowDatasetUncheckedUpgrade && instances.contains(datasetInstanceId.getNamespace(), datasetInstanceId)) {
        throw new InstanceConflictException(String.format("Dataset instance '%s' already exists.", datasetInstanceId));
      }

      DatasetDefinitionRegistry registry = getRegistryForType(datasetInstanceId.getNamespace(), datasetType);
      if (registry == null) {
        throw new DatasetManagementException(
          String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                        datasetType, datasetInstanceId.getNamespaceId()));
      }
      DatasetDefinition def = registry.get(datasetType);
      DatasetSpecification spec = def.configure(datasetInstanceId.getId(), props);
      def.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec, null).create();
      instances.put(datasetInstanceId.getNamespace(), datasetInstanceId, spec);
      LOG.info("Created dataset {} of type {}", datasetInstanceId, datasetType);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    writeLock.lock();
    try {
      DatasetSpecification oldSpec = instances.get(datasetInstanceId.getNamespace(), datasetInstanceId);
      if (oldSpec == null) {
        throw new InstanceConflictException(String.format("Dataset instance '%s' does not exist.", datasetInstanceId));
      }
      String datasetType = oldSpec.getType();
      DatasetDefinitionRegistry registry = getRegistryForType(datasetInstanceId.getNamespace(), datasetType);
      if (registry == null) {
        throw new DatasetManagementException(
          String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                        datasetType, datasetInstanceId.getNamespaceId()));
      }
      DatasetDefinition def = registry.get(datasetType);
      DatasetSpecification spec = def.configure(datasetInstanceId.getId(), props);
      instances.put(datasetInstanceId.getNamespace(), datasetInstanceId, spec);
      def.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec, null).upgrade();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId) {
    readLock.lock();
    try {
      // don't expect this to be called a lot.
      // might be better to maintain this collection separately and just return it, but seems like its not worth it.
      Collection<DatasetSpecification> specs = instances.row(namespaceId).values();
      ImmutableList.Builder<DatasetSpecificationSummary> specSummaries = ImmutableList.builder();
      for (DatasetSpecification spec : specs) {
        specSummaries.add(new DatasetSpecificationSummary(spec.getName(), spec.getType(), spec.getProperties()));
      }
      return specSummaries.build();
    } finally {
      readLock.unlock();
    }
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) {
    readLock.lock();
    try {
      return instances.get(datasetInstanceId.getNamespace(), datasetInstanceId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) {
    readLock.lock();
    try {
      return instances.contains(datasetInstanceId.getNamespace(), datasetInstanceId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean hasSystemType(String typeName) {
    return hasType(Id.DatasetType.from(Constants.SYSTEM_NAMESPACE, typeName));
  }

  @VisibleForTesting
  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) {
    return registries.containsKey(datasetTypeId.getNamespace()) &&
      registries.get(datasetTypeId.getNamespace()).hasType(datasetTypeId.getTypeName());
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId)
    throws DatasetManagementException, IOException {
    writeLock.lock();
    try {
      DatasetSpecification spec = instances.remove(datasetInstanceId.getNamespace(), datasetInstanceId);
      String datasetType = spec.getType();
      DatasetDefinitionRegistry registry = getRegistryForType(datasetInstanceId.getNamespace(), datasetType);
      if (registry == null) {
        throw new DatasetManagementException(
          String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                        datasetType, datasetInstanceId.getNamespaceId()));
      }
      DatasetDefinition def = registry.get(datasetType);
      def.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec, null).drop();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    writeLock.lock();
    try {
      for (DatasetSpecification spec : instances.row(namespaceId).values()) {
        String datasetType = spec.getType();
        DatasetDefinitionRegistry registry = getRegistryForType(namespaceId, datasetType);
        if (registry == null) {
          throw new DatasetManagementException(
            String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                          datasetType, namespaceId));
        }
        DatasetDefinition def = registry.get(spec.getType());
        def.getAdmin(DatasetContext.from(namespaceId.getId()), spec, null).drop();
      }
      instances.row(namespaceId).clear();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                                          @Nullable ClassLoader classLoader) throws IOException {
    readLock.lock();
    try {
      DatasetSpecification spec = instances.get(datasetInstanceId.getNamespace(), datasetInstanceId);
      if (spec == null) {
        return null;
      }
      LinkedHashSet<String> availableModuleClasses = getAvailableModuleClasses(datasetInstanceId.getNamespace());
      DatasetDefinition impl = createRegistry(availableModuleClasses, classLoader).get(spec.getType());
      return (T) impl.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec, classLoader);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          @Nullable Iterable<? extends Id> owners) throws IOException {
    readLock.lock();
    try {
      DatasetSpecification spec = instances.get(datasetInstanceId.getNamespace(), datasetInstanceId);
      if (spec == null) {
        return null;
      }
      LinkedHashSet<String> availableModuleClasses = getAvailableModuleClasses(datasetInstanceId.getNamespace());
      DatasetDefinition def = createRegistry(availableModuleClasses, classLoader).get(spec.getType());
      return (T) (def.getDataset(DatasetContext.from(datasetInstanceId.getNamespaceId()),
                                 spec, arguments, classLoader));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader) throws IOException {
    return getDataset(datasetInstanceId, arguments, classLoader, null);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners) throws IOException {
    readLock.lock();
    try {
      DatasetSpecification spec = instances.get(datasetInstanceId.getNamespace(), datasetInstanceId);
      if (spec == null) {
        return null;
      }
      LinkedHashSet<String> availableModuleClasses = getAvailableModuleClasses(datasetInstanceId.getNamespace());
      DatasetDefinition def =
        createRegistry(availableModuleClasses, classLoaderProvider.getParent()).get(spec.getType());
      return (T) (def.getDataset(DatasetContext.from(datasetInstanceId.getNamespaceId()),
        spec, arguments, classLoaderProvider.getParent()));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    writeLock.lock();
    try {
      if (!namespaces.add(namespaceId)) {
        throw new DatasetManagementException(String.format("Namespace %s already exists.", namespaceId.getId()));
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    writeLock.lock();
    try {
      Preconditions.checkArgument(!Constants.SYSTEM_NAMESPACE_ID.equals(namespaceId),
                                  "Cannot delete system namespace.");
      if (!namespaces.remove(namespaceId)) {
        throw new DatasetManagementException(String.format("Namespace %s does not exist", namespaceId.getId()));
      }
      instances.row(namespaceId).clear();
      moduleClasses.row(namespaceId).clear();
      registries.remove(namespaceId);
    } finally {
      writeLock.unlock();
    }
  }

  // because there may be dependencies between modules, it is important that they are ordered correctly.
  protected DatasetDefinitionRegistry createRegistry(LinkedHashSet<String> availableModuleClasses,
                                                   @Nullable ClassLoader classLoader) {
    DatasetDefinitionRegistry registry = registryFactory.create();
    for (String moduleClassName : availableModuleClasses) {
      // todo: this module loading and registering code somewhat duplicated in RemoteDatasetFramework
      Class<?> moduleClass;

      // try program class loader then cdap class loader
      try {
        moduleClass = ClassLoaders.loadClass(moduleClassName, classLoader, this);
      } catch (ClassNotFoundException e) {
        try {
          moduleClass = ClassLoaders.loadClass(moduleClassName, null, this);
        } catch (ClassNotFoundException e2) {
          LOG.error("Was not able to load dataset module class {}", moduleClassName, e);
          throw Throwables.propagate(e);
        }
      }

      try {
        DatasetModule module = DatasetModules.getDatasetModule(moduleClass);
        module.register(registry);
      } catch (Exception e) {
        LOG.error("Was not able to load dataset module class {}", moduleClassName, e);
        throw Throwables.propagate(e);
      }
    }

    return registry;
  }

  // gets all module class names available in the given namespace. Includes system modules first, then
  // namespace modules.
  protected LinkedHashSet<String> getAvailableModuleClasses(Id.Namespace namespace) {
    // order is important, system
    LinkedHashSet<String> availableModuleClasses = Sets.newLinkedHashSet();
    availableModuleClasses.addAll(moduleClasses.row(Constants.SYSTEM_NAMESPACE_ID).values());
    availableModuleClasses.addAll(moduleClasses.row(namespace).values());
    return availableModuleClasses;
  }

  @Nullable
  private DatasetDefinitionRegistry getRegistryForType(Id.Namespace namespaceId, String datasetType) {
    DatasetDefinitionRegistry registry = registries.get(namespaceId);
    if (registry != null && registry.hasType(datasetType)) {
      return registry;
    }
    registry = registries.get(Constants.SYSTEM_NAMESPACE_ID);
    if (registry != null && registry.hasType(datasetType)) {
      return registry;
    }
    return null;
  }

  // NOTE: this class is needed to collect all types added by a module
  private class TypesTrackingRegistry implements DatasetDefinitionRegistry {
    private final DatasetDefinitionRegistry delegate;

    private final List<String> types = Lists.newArrayList();

    public TypesTrackingRegistry(DatasetDefinitionRegistry delegate) {
      this.delegate = delegate;
    }

    public List<String> getTypes() {
      return types;
    }

    @Override
    public void add(DatasetDefinition def) {
      delegate.add(def);
      types.add(def.getName());
    }

    @Override
    public <T extends DatasetDefinition> T get(String datasetTypeName) {
      // In real-world scenarios, default modules are guaranteed to always exist in the system namespace.
      // Hence, we could add a preconditions check here to verify that registries contains types from system namespace
      // However, a lot of our tests (DatasetFrameworkTestUtil) start without default modules, so not adding that check.
      // In any case, the pattern here of first looking for the definition in own namespace, then in system is valid
      // and the else block should throw an exception if the dataset type is not found in the current or the system
      // namespace.
      if (delegate.hasType(datasetTypeName)) {
        return delegate.get(datasetTypeName);
      } else if (registries.containsKey(Constants.SYSTEM_NAMESPACE_ID)) {
        return registries.get(Constants.SYSTEM_NAMESPACE_ID).get(datasetTypeName);
      } else {
        throw new IllegalStateException(String.format("Dataset type %s not found.", datasetTypeName));
      }
    }

    @Override
    public boolean hasType(String datasetTypeName) {
      return delegate.hasType(datasetTypeName);
    }
  }
}
