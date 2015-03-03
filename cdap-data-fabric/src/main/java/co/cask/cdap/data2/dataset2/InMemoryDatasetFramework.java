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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A simple implementation of {@link co.cask.cdap.data2.dataset2.DatasetFramework} that keeps its state in
 * memory
 */
public class InMemoryDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDatasetFramework.class);

  private DatasetDefinitionRegistryFactory registryFactory;
  private Map<String, ? extends DatasetModule> defaultModules;

  private final Map<Id.DatasetModule, String> moduleClasses = Maps.newLinkedHashMap();
  private final Set<String> defaultTypes = Sets.newHashSet();
  private final Map<Id.Namespace, List<String>> nonDefaultTypes = Maps.newHashMap();
  private final Map<Id.DatasetInstance, DatasetSpecification> instances = Maps.newHashMap();
  private final List<Id.Namespace> namespaces = Lists.newArrayList();

  // NOTE: used only for "internal" operations, that doesn't return to client object of custom type
  // NOTE: for getting dataset/admin objects we construct fresh new one using all modules (no dependency management in
  //       this in-mem implementation for now) and passed client (program) classloader
  // NOTE: We maintain one DatasetDefinitionRegistry per namespace
  private Map<Id.Namespace, DatasetDefinitionRegistry> registries = Maps.newLinkedHashMap();

  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory) {
    this(registryFactory, new HashMap<String, DatasetModule>());
  }

  @Inject
  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                                  @Named("defaultDatasetModules") Map<String, ? extends DatasetModule> defaultModules) {
    this.registryFactory = registryFactory;
    this.defaultModules = defaultModules;
    resetRegistry();
  }

  @Override
  public synchronized void addModule(Id.DatasetModule moduleId,
                                     DatasetModule module) throws ModuleConflictException {
    if (moduleClasses.containsKey(moduleId)) {
      throw new ModuleConflictException("Cannot add module " + moduleId + ": it already exists.");
    }
    add(moduleId, module, false);
  }

  @Override
  public synchronized void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    // todo: check if existnig datasets or modules use this module
    moduleClasses.remove(moduleId);
    List<String> availableModuleClasses = getAvailableModuleClasses(moduleId.getNamespace());
    // this will cleanup types
    DatasetDefinitionRegistry registry = createRegistry(availableModuleClasses, registries.getClass().getClassLoader());
    registries.put(moduleId.getNamespace(), registry);
  }

  @Override
  public synchronized void deleteAllModules(Id.Namespace namespaceId) throws ModuleConflictException {
    // check if there are any datasets that use types from the namespace from which we want to remove all modules
    List<String> typesInNamespace = nonDefaultTypes.get(namespaceId);
    for (DatasetSpecification spec : instances.values()) {
      if (typesInNamespace.contains(spec.getType())) {
        throw new ModuleConflictException("Cannot delete all modules, some datasets use them");
      }
    }
    resetRegistry();
  }

  @Override
  public synchronized void addInstance(String datasetType, Id.DatasetInstance datasetInstanceId,
                                       DatasetProperties props) throws InstanceConflictException, IOException {
    if (instances.get(datasetInstanceId) != null) {
      throw new InstanceConflictException("Dataset instance with name already exists: " + datasetInstanceId);
    }

    DatasetDefinitionRegistry registry = getRegistryForType(datasetInstanceId.getNamespace(), datasetType);
    Preconditions.checkArgument(registry != null, "Dataset type '%s' is not registered", datasetType);
    DatasetDefinition def = registry.get(datasetType);
    DatasetSpecification spec = def.configure(datasetInstanceId.getId(), props);
    def.getAdmin(new DatasetContext(datasetInstanceId.getNamespaceId()), null, spec).create();
    instances.put(datasetInstanceId, spec);
    LOG.info("Created dataset {} of type {}", datasetInstanceId, datasetType);
  }

  @Override
  public synchronized void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws InstanceConflictException, IOException {
    DatasetSpecification oldSpec = instances.get(datasetInstanceId);
    if (oldSpec == null) {
      throw new InstanceConflictException("Dataset instance with name does not exist: " + datasetInstanceId);
    }
    String datasetType = oldSpec.getType();
    DatasetDefinitionRegistry registry = getRegistryForType(datasetInstanceId.getNamespace(), datasetType);
    Preconditions.checkArgument(registry != null, "Dataset type '%s' is not registered", datasetType);
    DatasetDefinition def = registry.get(datasetType);
    DatasetSpecification spec = def.configure(datasetInstanceId.getId(), props);
    instances.put(datasetInstanceId, spec);
    def.getAdmin(new DatasetContext(datasetInstanceId.getNamespaceId()), null, spec).upgrade();
  }

  @Override
  public synchronized Collection<DatasetSpecification> getInstances(Id.Namespace namespaceId) {
    return Collections.unmodifiableCollection(instances.values());
  }

  @Nullable
  @Override
  public synchronized DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId)
    throws DatasetManagementException {
    return instances.get(datasetInstanceId);
  }

  @Override
  public synchronized boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return instances.containsKey(datasetInstanceId);
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return hasType(Id.DatasetType.from(Constants.SYSTEM_NAMESPACE, typeName));
  }

  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    if (registries.containsKey(datasetTypeId.getNamespace())) {
      return registries.get(datasetTypeId.getNamespace()).hasType(datasetTypeId.getTypeName());
    }
    return false;
  }

  @Override
  public synchronized void deleteInstance(Id.DatasetInstance datasetInstanceId)
    throws InstanceConflictException, IOException {
    DatasetSpecification spec = instances.remove(datasetInstanceId);
    String datasetType = spec.getType();
    DatasetDefinitionRegistry registry = getRegistryForType(datasetInstanceId.getNamespace(), datasetType);
    Preconditions.checkState(registry != null, "Dataset type '%s' is not registered", datasetType);
    DatasetDefinition def = registry.get(spec.getType());
    def.getAdmin(new DatasetContext(datasetInstanceId.getNamespaceId()), null, spec).drop();
  }

  @Override
  public synchronized void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    for (DatasetSpecification spec : instances.values()) {
      String datasetType = spec.getType();
      DatasetDefinitionRegistry registry = getRegistryForType(namespaceId, datasetType);
      Preconditions.checkState(registry != null, "Dataset type '%s' is not registered", datasetType);
      DatasetDefinition def = registry.get(spec.getType());
      def.getAdmin(new DatasetContext(namespaceId.getId()), null, spec).drop();
    }
    instances.clear();
  }

  @Override
  public synchronized <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId,
                                                          @Nullable ClassLoader classLoader)
    throws IOException {

    DatasetSpecification spec = instances.get(datasetInstanceId);
    if (spec == null) {
      return null;
    }
    List<String> availableModuleClasses = getAvailableModuleClasses(datasetInstanceId.getNamespace());
    DatasetDefinition impl = createRegistry(availableModuleClasses, classLoader).get(spec.getType());
    return (T) impl.getAdmin(new DatasetContext(datasetInstanceId.getNamespaceId()), classLoader, spec);
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                                       Map<String, String> arguments,
                                                       @Nullable ClassLoader classLoader) throws IOException {

    DatasetSpecification spec = instances.get(datasetInstanceId);
    if (spec == null) {
      return null;
    }
    List<String> availableModuleClasses = getAvailableModuleClasses(datasetInstanceId.getNamespace());
    DatasetDefinition def = createRegistry(availableModuleClasses, classLoader).get(spec.getType());
    return (T) (def.getDataset(new DatasetContext(datasetInstanceId.getNamespaceId()), arguments, classLoader, spec));
  }

  private DatasetDefinitionRegistry createRegistry(List<String> availableModuleClasses,
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

  private List<String> getAvailableModuleClasses(Id.Namespace namespace) {
    LinkedList<String> allClasses = getAllModuleClasses(Constants.SYSTEM_NAMESPACE_ID);
    if (Constants.SYSTEM_NAMESPACE_ID.equals(namespace)) {
      return allClasses;
    }
    LinkedList<String> namespaceClasses = getAllModuleClasses(namespace);
    for (String namespaceClass : namespaceClasses) {
      int idx = allClasses.indexOf(namespaceClass);
      if (idx > -1) {
        // This applies when you have the same type class in the system namespace and the current module's namespace.
        // In such a scenario, when you reach here, allClasses has type classes from modules in the system namespace.
        // While iterating over the type classes from modules in the current namespace, if you encounter a class that
        // is already present in allClasses, remove it, then add the corresponding version of the class from the module
        // in the current namespace. This is to give type classes in the current namespace precedence over type
        // classes in the system namespace.
        allClasses.remove(idx);
        allClasses.add(idx, namespaceClass);
      } else {
        allClasses.addLast(namespaceClass);
      }
    }
    return allClasses;
  }

  private LinkedList<String> getAllModuleClasses(Id.Namespace namespace) {
    Set<String> classes = Sets.newHashSet();
    LinkedList<String> classesl = Lists.newLinkedList();
    for (Map.Entry<Id.DatasetModule, String> entry : moduleClasses.entrySet()) {
      if (entry.getKey().getNamespace().equals(namespace)) {
        classes.add(entry.getValue());
        classesl.addLast(entry.getValue());
      }
    }
    return classesl;
  }

  private void add(Id.DatasetModule moduleId, DatasetModule module, boolean defaultModule) {
    DatasetDefinitionRegistry registry;
    if (registries.containsKey(moduleId.getNamespace())) {
      registry = registries.get(moduleId.getNamespace());
    } else {
      registry = registryFactory.create();
    }
    TypesTrackingRegistry trackingRegistry = new TypesTrackingRegistry(registry);
    module.register(trackingRegistry);
    if (defaultModule) {
      defaultTypes.addAll(trackingRegistry.getTypes());
    } else {
      List<String> existingTypesInNamespace = nonDefaultTypes.get(moduleId.getNamespace());
      if (existingTypesInNamespace == null) {
        existingTypesInNamespace = Lists.newArrayList();
      }
      existingTypesInNamespace.addAll(trackingRegistry.getTypes());
      nonDefaultTypes.put(moduleId.getNamespace(), existingTypesInNamespace);
    }
    moduleClasses.put(moduleId, DatasetModules.getDatasetModuleClass(module).getName());
    registries.put(moduleId.getNamespace(), registry);
  }

  private void resetRegistry() {
    LOG.info("RESET " + this.toString());

    moduleClasses.clear();
    defaultTypes.clear();
    registries.clear();
    for (Map.Entry<String, ? extends DatasetModule> entry : defaultModules.entrySet()) {
      LOG.info("Adding Default module: " + entry.getKey() + " " + this.toString());
      // default modules in system namespace
      add(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, entry.getKey()), entry.getValue(), true);
    }
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

  @Override
  public synchronized void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    if (namespaces.contains(namespaceId)) {
      throw new DatasetManagementException(String.format("Namespace %s already exists.", namespaceId.getId()));
    }
    namespaces.add(namespaceId);
  }

  @Override
  public synchronized void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    if (!namespaces.contains(namespaceId)) {
      throw new DatasetManagementException(String.format("Namespace %s does not exist", namespaceId.getId()));
    }
    namespaces.remove(namespaceId);
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
      // However, a lot of our tests (AbstractDatasetTest) start without default modules, so not adding that check.
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
