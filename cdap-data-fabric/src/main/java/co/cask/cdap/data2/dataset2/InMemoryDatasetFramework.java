/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
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

  private final Map<String, String> moduleClasses = Maps.newLinkedHashMap();
  private final Set<String> defaultTypes = Sets.newHashSet();
  private final Map<String, DatasetSpecification> instances = Maps.newHashMap();
  private final boolean allowDatasetUncheckedUpgrade;

  // NOTE: used only for "internal" operations, that doesn't return to client object of custom type
  // NOTE: for getting dataset/admin objects we construct fresh new one using all modules (no dependency management in
  //       this in-mem implementation for now) and passed client (program) classloader
  private DatasetDefinitionRegistry registry;

  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory, CConfiguration configuration) {
    this(registryFactory, new HashMap<String, DatasetModule>(), configuration);
  }

  @Inject
  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                                  @Named("defaultDatasetModules") Map<String, ? extends DatasetModule> defaultModules,
                                  CConfiguration configuration) {
    this.registryFactory = registryFactory;
    this.defaultModules = defaultModules;
    this.allowDatasetUncheckedUpgrade = configuration.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
    resetRegistry();
  }

  @Override
  public synchronized void addModule(String moduleName, DatasetModule module)
    throws ModuleConflictException {

    if (moduleClasses.containsKey(moduleName) && !allowDatasetUncheckedUpgrade) {
      throw new ModuleConflictException("Cannot add module " + moduleName + ": it already exists.");
    }
    add(moduleName, module, false);
  }

  @Override
  public synchronized void deleteModule(String moduleName) throws ModuleConflictException {
    // todo: check if existnig datasets or modules use this module
    moduleClasses.remove(moduleName);
    // this will cleanup types
    registry = createRegistry(registry.getClass().getClassLoader());
  }

  @Override
  public synchronized void deleteAllModules() throws DatasetManagementException {
    // check if there are any datasets that use non-default types that we want to remove
    for (DatasetSpecification spec : instances.values()) {
      if (!defaultTypes.contains(spec.getType())) {
        throw new ModuleConflictException("Cannot delete all modules, some datasets use them");
      }
    }
    resetRegistry();
  }

  @Override
  public synchronized void addInstance(String datasetType, String datasetInstanceName, DatasetProperties props)
    throws InstanceConflictException, IOException {
    if (!allowDatasetUncheckedUpgrade && instances.get(datasetInstanceName) != null) {
      throw new InstanceConflictException("Dataset instance with name already exists: " + datasetInstanceName);
    }

    Preconditions.checkArgument(registry.hasType(datasetType), "Dataset type '%s' is not registered", datasetType);
    DatasetDefinition def = registry.get(datasetType);
    DatasetSpecification spec = def.configure(datasetInstanceName, props);
    instances.put(datasetInstanceName, spec);
    def.getAdmin(spec, null).create();
    instances.put(datasetInstanceName, spec);
    LOG.info("Created dataset {} of type {}", datasetInstanceName, datasetType);
  }

  @Override
  public synchronized void updateInstance(String datasetInstanceName, DatasetProperties props)
    throws InstanceConflictException, IOException {
    DatasetSpecification oldSpec = instances.get(datasetInstanceName);
    if (oldSpec == null) {
      throw new InstanceConflictException("Dataset instance with name does not exist: " + datasetInstanceName);
    }
    String datasetType = oldSpec.getType();
    Preconditions.checkArgument(registry.hasType(datasetType), "Dataset type '%s' is not registered", datasetType);
    DatasetDefinition def = registry.get(datasetType);
    DatasetSpecification spec = def.configure(datasetInstanceName, props);
    instances.put(datasetInstanceName, spec);
    def.getAdmin(spec, null).upgrade();
  }

  @Override
  public synchronized Collection<DatasetSpecification> getInstances() {
    return Collections.unmodifiableCollection(instances.values());
  }

  @Nullable
  @Override
  public synchronized DatasetSpecification getDatasetSpec(String name) throws DatasetManagementException {
    return instances.get(name);
  }

  @Override
  public synchronized boolean hasInstance(String instanceName) throws DatasetManagementException {
    return instances.containsKey(instanceName);
  }

  @Override
  public synchronized boolean hasType(String typeName) throws DatasetManagementException {
    return registry.hasType(typeName);
  }

  @Override
  public synchronized void deleteInstance(String datasetInstanceName) throws InstanceConflictException, IOException {
    DatasetSpecification spec = instances.remove(datasetInstanceName);
    DatasetDefinition def = registry.get(spec.getType());
    def.getAdmin(spec, null).drop();
  }

  @Override
  public synchronized void deleteAllInstances() throws DatasetManagementException, IOException {
    for (DatasetSpecification spec : instances.values()) {
      DatasetDefinition def = registry.get(spec.getType());
      def.getAdmin(spec, null).drop();
    }
    instances.clear();
  }

  @Override
  public synchronized <T extends DatasetAdmin> T getAdmin(String datasetInstanceName,
                                                          @Nullable ClassLoader classLoader)
    throws IOException {

    DatasetSpecification spec = instances.get(datasetInstanceName);
    if (spec == null) {
      return null;
    }
    DatasetDefinition impl = createRegistry(classLoader).get(spec.getType());
    return (T) impl.getAdmin(spec, classLoader);
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String datasetInstanceName,
                                                       Map<String, String> arguments,
                                                       @Nullable ClassLoader classLoader)
    throws IOException {

    DatasetSpecification spec = instances.get(datasetInstanceName);
    if (spec == null) {
      return null;
    }
    DatasetDefinition def = createRegistry(classLoader).get(spec.getType());
    return (T) (def.getDataset(spec, arguments, classLoader));
  }

  private DatasetDefinitionRegistry createRegistry(@Nullable ClassLoader classLoader) {
    DatasetDefinitionRegistry registry = registryFactory.create();
    for (String moduleClassName : moduleClasses.values()) {
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

  private void add(String moduleName, DatasetModule module, boolean defaultModule) {
    TypesTrackingRegistry trackingRegistry = new TypesTrackingRegistry(registry);
    module.register(trackingRegistry);
    if (defaultModule) {
      defaultTypes.addAll(trackingRegistry.getTypes());
    }
    moduleClasses.put(moduleName, DatasetModules.getDatasetModuleClass(module).getName());
  }

  private void resetRegistry() {
    LOG.info("RESET " + this.toString());

    moduleClasses.clear();
    defaultTypes.clear();
    registry = registryFactory.create();
    for (Map.Entry<String, ? extends DatasetModule> entry : defaultModules.entrySet()) {
      LOG.info("Adding Default module: " + entry.getKey() + " " + this.toString());
      add(entry.getKey(), entry.getValue(), true);
    }
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
      return delegate.get(datasetTypeName);
    }

    @Override
    public boolean hasType(String datasetTypeName) {
      return delegate.hasType(datasetTypeName);
    }
  }
}
