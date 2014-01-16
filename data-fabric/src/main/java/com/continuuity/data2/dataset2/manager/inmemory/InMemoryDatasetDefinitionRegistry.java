package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.api.data.dataset2.DatasetDefinition;
import com.continuuity.api.data.module.DatasetConfigurator;
import com.continuuity.api.data.module.DatasetDefinitionRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Simple implementation of {@link DatasetDefinitionRegistry} that keeps state in memory.
 */
public class InMemoryDatasetDefinitionRegistry implements DatasetDefinitionRegistry {
  private Map<String, DatasetDefinition> datasetTypes = Maps.newHashMap();
  private List<DatasetConfigurator> configurators = Lists.newArrayList();

  @Override
  public <T extends DatasetDefinition> T get(String datasetType) {
    return (T) datasetTypes.get(datasetType);
  }

  @Override
  public void add(DatasetDefinition def) {
    configure(def);
    datasetTypes.put(def.getName(), def);
  }

  @Override
  public void add(DatasetConfigurator configurator) {
    configurators.add(configurator);
  }

  /**
   * Must be called before passing it to the next {@link com.continuuity.api.data.module.DatasetModule} to reset
   * {@link DatasetConfigurator}s to be applied to the {@link DatasetDefinition}s added by the next module. I.e. to
   * avoid {@link DatasetConfigurator}s from one module to be applied to the {@link DatasetDefinition}s from another
   * module when using same registry to register both modules.
   */
  public void nextModule() {
    configurators.clear();
  }

  private void configure(DatasetDefinition def) {
    for (DatasetConfigurator configurator : configurators) {
      boolean stop = !configurator.configure(def);
      if (stop) {
        break;
      }
    }
  }
}
