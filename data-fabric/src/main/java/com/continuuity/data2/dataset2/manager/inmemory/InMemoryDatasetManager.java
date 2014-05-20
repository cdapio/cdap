package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.InstanceConflictException;
import com.continuuity.data2.dataset2.manager.ModuleConflictException;
import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * A simple implementation of {@link DatasetManager} that keeps its state in memory
 */
public class InMemoryDatasetManager implements DatasetManager {
  private final Set<String> modules = Sets.newHashSet();
  private final DatasetDefinitionRegistry registry;
  private final Map<String, DatasetInstanceSpec> instances = Maps.newHashMap();

  public InMemoryDatasetManager() {
    this(new InMemoryDatasetDefinitionRegistry());
  }

  @Inject
  public InMemoryDatasetManager(DatasetDefinitionRegistry registry) {
    this.registry = registry;
  }

  @Override
  public synchronized void register(String moduleName, Class<? extends DatasetModule> moduleClass)
    throws ModuleConflictException {

    if (modules.contains(moduleName)) {
      throw new ModuleConflictException("Cannot add module " + moduleName + ": it already exists.");
    }
    try {
      DatasetModule module = moduleClass.newInstance();
      module.register(registry);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void deleteModule(String moduleName) throws ModuleConflictException {
    modules.remove(moduleName);
    // todo: remove from registry & check for conflicts. It is fine for now, as we don't use delete with in-mem version
  }

  @Override
  public synchronized void addInstance(String datasetType, String datasetInstanceName, DatasetInstanceProperties props)
    throws InstanceConflictException {
    if (instances.get(datasetInstanceName) != null) {
      throw new InstanceConflictException("Dataset instance with name already exists: " + datasetInstanceName);
    }

    DatasetDefinition def = registry.get(datasetType);
    instances.put(datasetInstanceName, def.configure(datasetInstanceName, props));
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws InstanceConflictException {
    instances.remove(datasetInstanceName);
  }

  @Override
  public synchronized <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws IOException {

    DatasetInstanceSpec spec = instances.get(datasetInstanceName);
    if (spec == null) {
      return null;
    }
    DatasetDefinition impl = registry.get(spec.getType());
    return (T) impl.getAdmin(spec);
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String datasetInstanceName, ClassLoader ignored)
    throws IOException {

    DatasetInstanceSpec spec = instances.get(datasetInstanceName);
    if (spec == null) {
      return null;
    }
    DatasetDefinition impl = registry.get(spec.getType());
    return (T) impl.getDataset(spec);
  }
}
