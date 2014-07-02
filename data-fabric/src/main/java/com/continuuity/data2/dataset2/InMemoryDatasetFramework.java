package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A simple implementation of {@link com.continuuity.data2.dataset2.DatasetFramework} that keeps its state in
 * memory
 */
public class InMemoryDatasetFramework implements DatasetFramework {
  private final Set<String> modules = Sets.newHashSet();
  private final DatasetDefinitionRegistryFactory registryFactory;
  private final Map<String, DatasetSpecification> instances = Maps.newHashMap();

  private DatasetDefinitionRegistry registry;

  @Inject
  public InMemoryDatasetFramework(DatasetDefinitionRegistryFactory registryFactory) {
    this.registryFactory = registryFactory;
    this.registry = registryFactory.create();
  }

  @Override
  public synchronized void addModule(String moduleName, DatasetModule module)
    throws ModuleConflictException {

    if (modules.contains(moduleName)) {
      throw new ModuleConflictException("Cannot add module " + moduleName + ": it already exists.");
    }
    module.register(registry);
  }

  @Override
  public void deleteModule(String moduleName) throws ModuleConflictException {
    modules.remove(moduleName);
    // todo: remove from registry & check for conflicts. It is fine for now, as we don't use delete with in-mem version
  }

  @Override
  public void deleteAllModules() throws DatasetManagementException {
    if (!instances.isEmpty()) {
      // todo: quick check - not enough though
      throw new ModuleConflictException("Cannot delete all modules, some datasets use them");
    }
    modules.clear();
    registry = registryFactory.create();
    // todo: remove from registry & check for conflicts. It is fine for now, as we don't use delete with in-mem version
  }

  @Override
  public synchronized void addInstance(String datasetType, String datasetInstanceName, DatasetProperties props)
    throws InstanceConflictException, IOException {
    if (instances.get(datasetInstanceName) != null) {
      throw new InstanceConflictException("Dataset instance with name already exists: " + datasetInstanceName);
    }

    DatasetDefinition def = registry.get(datasetType);
    Preconditions.checkNotNull(def, "Dataset type '%s' is not registered", datasetType);
    DatasetSpecification spec = def.configure(datasetInstanceName, props);
    instances.put(datasetInstanceName, spec);
    def.getAdmin(spec, null).create();
    instances.put(datasetInstanceName, spec);
  }

  @Override
  public Collection<DatasetSpecification> getInstances() {
    return Collections.unmodifiableCollection(instances.values());
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(String name) throws DatasetManagementException {
    return instances.get(name);
  }

  @Override
  public boolean hasInstance(String instanceName) throws DatasetManagementException {
    return instances.containsKey(instanceName);
  }

  @Override
  public boolean hasType(String typeName) throws DatasetManagementException {
    return registry.get(typeName) != null;
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws InstanceConflictException, IOException {
    DatasetSpecification spec = instances.remove(datasetInstanceName);
    DatasetDefinition def = registry.get(spec.getType());
    def.getAdmin(spec, null).create();
  }

  @Override
  public void deleteAllInstances() throws DatasetManagementException, IOException {
    instances.clear();
  }

  @Override
  public synchronized <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws IOException {

    DatasetSpecification spec = instances.get(datasetInstanceName);
    if (spec == null) {
      return null;
    }
    DatasetDefinition impl = registry.get(spec.getType());
    return (T) impl.getAdmin(spec, classLoader);
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String datasetInstanceName, ClassLoader classLoader)
    throws IOException {

    DatasetSpecification spec = instances.get(datasetInstanceName);
    if (spec == null) {
      return null;
    }
    DatasetDefinition impl = registry.get(spec.getType());
    return (T) impl.getDataset(spec, classLoader);
  }
}
