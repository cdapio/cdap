package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.api.data.dataset2.Dataset;
import com.continuuity.api.data.dataset2.DatasetDefinition;
import com.continuuity.api.data.dataset2.DatasetAdmin;
import com.continuuity.api.data.dataset2.DatasetInstanceProperties;
import com.continuuity.api.data.dataset2.DatasetInstanceSpec;
import com.continuuity.api.data.module.DatasetModule;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * A simple implementation of {@link DatasetManager} that keeps its state in memory
 */
public class InMemoryDatasetManager implements DatasetManager {
  private final InMemoryDatasetDefinitionRegistry registry = new InMemoryDatasetDefinitionRegistry();
  private final Map<String, DatasetInstanceSpec> instances = Maps.newHashMap();

  @Override
  public synchronized void register(String moduleName, Class<? extends DatasetModule> module) {
    register(Lists.<Class<? extends DatasetModule>>newArrayList(module));
  }

  @Override
  public synchronized void addInstance(String datasetType, String datasetInstanceName, DatasetInstanceProperties props)
    throws Exception {
    Preconditions.checkArgument(instances.get(datasetInstanceName) == null,
                                "Dataset instance with name {} already exists", datasetInstanceName);

    DatasetDefinition def = registry.get(datasetType);
    instances.put(datasetInstanceName, def.configure(datasetInstanceName, props));
  }

  @Override
  public synchronized <T extends DatasetAdmin> T getAdmin(String datasetInstanceName) throws Exception {
    DatasetInstanceSpec spec = instances.get(datasetInstanceName);
    DatasetDefinition impl = registry.get(spec.getType());
    return (T) impl.getAdmin(spec);
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String datasetInstanceName) throws Exception {
    DatasetInstanceSpec spec = instances.get(datasetInstanceName);
    if (spec == null) {
      return null;
    }
    DatasetDefinition impl = registry.get(spec.getType());
    return (T) impl.getDataset(spec);
  }

  private void register(List<Class<? extends DatasetModule>> modules) {
    for (Class<? extends DatasetModule> moduleClass : modules) {
      try {
        DatasetModule module = moduleClass.newInstance();
        registry.nextModule();
        module.register(registry);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

}
