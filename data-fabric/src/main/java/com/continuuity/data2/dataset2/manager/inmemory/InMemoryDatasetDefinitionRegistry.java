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

  private void configure(DatasetDefinition def) {
    for (DatasetConfigurator configurator : configurators) {
      boolean stop = !configurator.configure(def);
      if (stop) {
        break;
      }
    }
  }
}
