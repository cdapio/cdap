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
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.dataset.Updatable;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
  private final Map<Id.DatasetType, DatasetTypeMeta> types;

  private final Lock readLock;
  private final Lock writeLock;
  
  // NOTE: used only for "internal" operations, that doesn't return to client object of custom type
  // NOTE: for getting dataset/admin objects we construct fresh new one using all modules (no dependency management in
  //       this in-mem implementation for now) and passed client (program) class loader
  // NOTE: We maintain one DatasetDefinitionRegistry per namespace
  private final Map<Id.Namespace, DatasetDefinitionRegistry> registries;

  private AuditPublisher auditPublisher;

  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory) {
    this(registryFactory, new HashMap<String, DatasetModule>());
  }

  @Inject
  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                                  @Named("defaultDatasetModules") Map<String, DatasetModule> defaultModules) {
    this.registryFactory = registryFactory;
    this.namespaces = Sets.newHashSet();
    this.nonDefaultTypes = HashMultimap.create();
    this.instances = HashBasedTable.create();
    this.types = Maps.newHashMap();
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
    namespaces.add(Id.Namespace.SYSTEM);
    DatasetDefinitionRegistry systemRegistry = registryFactory.create();
    for (Map.Entry<String, DatasetModule> entry : defaultModules.entrySet()) {
      LOG.debug("Adding Default module {} to system namespace", entry.getKey());
      String moduleName = entry.getKey();
      DatasetModule module = entry.getValue();
      entry.getValue().register(systemRegistry);
      // keep track of default module classes. These are used when creating registries for other namespaces,
      // which need to register system classes too.
      String moduleClassName = DatasetModules.getDatasetModuleClass(module).getName();
      Id.DatasetModule moduleId = Id.DatasetModule.from(Id.Namespace.SYSTEM, moduleName);
      moduleClasses.put(Id.Namespace.SYSTEM, moduleId, moduleClassName);
    }
    registries.put(Id.Namespace.SYSTEM, systemRegistry);

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module) throws ModuleConflictException {
    // TODO (CDAP-6297): check if existing modules overlap, or if this removes a type other modules depend on
    writeLock.lock();
    try {
      DatasetDefinitionRegistry registry = registries.get(moduleId.getNamespace());
      if (registry == null) {
        registry = registryFactory.create();
        registries.put(moduleId.getNamespace(), registry);
      }
      TypesTrackingRegistry trackingRegistry = new TypesTrackingRegistry(registry);
      module.register(trackingRegistry);
      String moduleClassName = DatasetModules.getDatasetModuleClass(module).getName();
      moduleClasses.put(moduleId.getNamespace(), moduleId, moduleClassName);
      List<String> types = trackingRegistry.getTypes();
      nonDefaultTypes.putAll(moduleId.getNamespace(), types);
      for (String type : types) {
        this.types.put(Id.DatasetType.from(moduleId.getNamespace(), type),
                       new DatasetTypeMeta(type, Collections.singletonList(
                         new DatasetModuleMeta(moduleId.getId(), moduleClassName, null,
                                               types, Collections.<String>emptyList()))));
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module,
                        Location jarLocation) throws DatasetManagementException {
    // Location is never used to create classloader for in memory dataset framework
    addModule(moduleId, module);
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) {
    // TODO (CDAP-6297): check if existing datasets or modules use this module
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
      if (instances.contains(datasetInstanceId.getNamespace(), datasetInstanceId)) {
        throw new InstanceConflictException(String.format("Dataset instance '%s' already exists.", datasetInstanceId));
      }

      DatasetDefinition def = getDefinitionForType(datasetInstanceId.getNamespace(), datasetType);
      if (def == null) {
        throw new DatasetManagementException(
          String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                        datasetType, datasetInstanceId.getNamespaceId()));
      }
      DatasetSpecification spec = def.configure(datasetInstanceId.getId(), props);
      spec = spec.setOriginalProperties(props);
      if (props.getDescription() != null) {
        spec = spec.setDescription(props.getDescription());
      }
      def.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec, null).create();
      instances.put(datasetInstanceId.getNamespace(), datasetInstanceId, spec);
      publishAudit(datasetInstanceId, AuditType.CREATE);
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
        throw new InstanceNotFoundException(datasetInstanceId.getId());
      }
      DatasetDefinition def = getDefinitionForType(datasetInstanceId.getNamespace(), oldSpec.getType());
      if (def == null) {
        throw new DatasetManagementException(
          String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                        oldSpec.getType(), datasetInstanceId.getNamespaceId()));
      }
      DatasetSpecification spec =
        AbstractDatasetDefinition.reconfigure(def, datasetInstanceId.getId(), props, oldSpec)
          .setOriginalProperties(props);
      if (props.getDescription() != null) {
        spec = spec.setDescription(props.getDescription());
      }
      instances.put(datasetInstanceId.getNamespace(), datasetInstanceId, spec);
      DatasetAdmin admin = def.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), spec, null);
      if (admin instanceof Updatable) {
        ((Updatable) admin).update(oldSpec);
      } else {
        admin.upgrade();
      }
      publishAudit(datasetInstanceId, AuditType.UPDATE);
    } catch (IncompatibleUpdateException e) {
      throw new InstanceConflictException("Update failed for dataset instance " + datasetInstanceId, e);
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
      DatasetSpecification spec = instances.get(datasetInstanceId.getNamespace(), datasetInstanceId);
      return DatasetsUtil.fixOriginalProperties(spec);
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
    return hasType(Id.DatasetType.from(Id.Namespace.SYSTEM, typeName));
  }

  @VisibleForTesting
  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) {
    return registries.containsKey(datasetTypeId.getNamespace()) &&
      registries.get(datasetTypeId.getNamespace()).hasType(datasetTypeId.getTypeName());
  }

  @Nullable
  @Override
  public DatasetTypeMeta getTypeInfo(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    return types.get(datasetTypeId);
  }

  @Override
  public void truncateInstance(Id.DatasetInstance instanceId)
    throws DatasetManagementException, IOException {
    writeLock.lock();
    try {
      DatasetSpecification spec = instances.get(instanceId.getNamespace(), instanceId);
      if (spec == null) {
        throw new InstanceNotFoundException(instanceId.getId());
      }
      DatasetDefinition def = getDefinitionForType(instanceId.getNamespace(), spec.getType());
      if (def == null) {
        throw new DatasetManagementException(
          String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                        spec.getType(), instanceId.getNamespaceId()));
      }
      def.getAdmin(DatasetContext.from(instanceId.getNamespaceId()), spec, null).truncate();
      publishAudit(instanceId, AuditType.TRUNCATE);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteInstance(Id.DatasetInstance instanceId)
    throws DatasetManagementException, IOException {
    writeLock.lock();
    try {
      DatasetSpecification spec = instances.remove(instanceId.getNamespace(), instanceId);
      if (spec == null) {
        throw new InstanceNotFoundException(instanceId.getId());
      }
      DatasetDefinition def = getDefinitionForType(instanceId.getNamespace(), spec.getType());
      if (def == null) {
        throw new DatasetManagementException(
          String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                        spec.getType(), instanceId.getNamespaceId()));
      }
      def.getAdmin(DatasetContext.from(instanceId.getNamespaceId()), spec, null).drop();
      publishAudit(instanceId, AuditType.DELETE);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    writeLock.lock();
    try {
      for (DatasetSpecification spec : instances.row(namespaceId).values()) {
        DatasetDefinition def = getDefinitionForType(namespaceId, spec.getType());
        if (def == null) {
          throw new DatasetManagementException(
            String.format("Dataset type '%s' is neither registered in the '%s' namespace nor in the system namespace",
                          spec.getType(), namespaceId));
        }
        def.getAdmin(DatasetContext.from(namespaceId.getId()), spec, null).drop();
        publishAudit(Id.DatasetInstance.from(namespaceId, spec.getName()), AuditType.DELETE);
      }
      instances.row(namespaceId).clear();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                             @Nullable ClassLoader classLoader) throws IOException {
    return getAdmin(datasetInstanceId, classLoader, new ConstantClassLoaderProvider(classLoader));
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                             @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider) throws IOException {
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
                                          @Nullable ClassLoader classLoader) throws IOException {
    return getDataset(datasetInstanceId, arguments, classLoader,
                      new ConstantClassLoaderProvider(classLoader), null, AccessType.UNKNOWN);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader parentClassLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners, AccessType accessType)
    throws IOException {

    readLock.lock();
    try {
      DatasetSpecification spec = instances.get(datasetInstanceId.getNamespace(), datasetInstanceId);
      if (spec == null) {
        return null;
      }
      LinkedHashSet<String> availableModuleClasses = getAvailableModuleClasses(datasetInstanceId.getNamespace());
      DatasetDefinition def =
        createRegistry(availableModuleClasses, parentClassLoader).get(spec.getType());
      return (T) (def.getDataset(DatasetContext.from(datasetInstanceId.getNamespaceId()),
                                 spec, arguments, parentClassLoader));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void writeLineage(Id.DatasetInstance datasetInstanceId, AccessType accessType) {
    // no-op. The InMemoryDatasetFramework doesn't need to do anything.
    // The lineage should be recorded before this point. In fact, this should not even be called because
    // RemoteDatasetFramework's implementation of this is also a no-op.
  }

  // because there may be dependencies between modules, it is important that they are ordered correctly.
  protected DatasetDefinitionRegistry createRegistry(LinkedHashSet<String> availableModuleClasses,
                                                   @Nullable ClassLoader classLoader) {
    DatasetDefinitionRegistry registry = registryFactory.create();
    for (String moduleClassName : availableModuleClasses) {
      try {
        DatasetDefinitionRegistries.register(moduleClassName, classLoader, registry);
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
    availableModuleClasses.addAll(moduleClasses.row(Id.Namespace.SYSTEM).values());
    availableModuleClasses.addAll(moduleClasses.row(namespace).values());
    return availableModuleClasses;
  }

  @Nullable
  @VisibleForTesting
  DatasetDefinition getDefinitionForType(Id.Namespace namespaceId, String datasetType) {
    DatasetDefinitionRegistry registry = registries.get(namespaceId);
    if (registry != null && registry.hasType(datasetType)) {
      return registry.get(datasetType);
    }
    registry = registries.get(Id.Namespace.SYSTEM);
    if (registry != null && registry.hasType(datasetType)) {
      return registry.get(datasetType);
    }
    return null;
  }

  // NOTE: this class is needed to collect all types added by a module
  private class TypesTrackingRegistry implements DatasetDefinitionRegistry {
    private final DatasetDefinitionRegistry delegate;

    private final List<String> types = Lists.newArrayList();

    TypesTrackingRegistry(DatasetDefinitionRegistry delegate) {
      this.delegate = delegate;
    }

    List<String> getTypes() {
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
      } else if (registries.containsKey(Id.Namespace.SYSTEM)) {
        return registries.get(Id.Namespace.SYSTEM).get(datasetTypeName);
      } else {
        throw new IllegalStateException(String.format("Dataset type %s not found.", datasetTypeName));
      }
    }

    @Override
    public boolean hasType(String datasetTypeName) {
      return delegate.hasType(datasetTypeName);
    }
  }

  private void publishAudit(Id.DatasetInstance datasetInstance, AuditType auditType) {
    // Don't publish audit for system datasets admin operations, there can be a deadlock
    if (NamespaceId.SYSTEM.getNamespace().equals(datasetInstance.getNamespaceId()) &&
      auditType != AuditType.ACCESS) {
      return;
    }
    AuditPublishers.publishAudit(auditPublisher, datasetInstance, auditType, AuditPayload.EMPTY_PAYLOAD);
  }
}
