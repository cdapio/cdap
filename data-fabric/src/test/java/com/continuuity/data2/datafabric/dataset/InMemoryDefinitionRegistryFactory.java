package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.InMemoryDatasetDefinitionRegistry;

/**
 *
 */
public class InMemoryDefinitionRegistryFactory implements DatasetDefinitionRegistryFactory {

  @Override
  public DatasetDefinitionRegistry create() {
    return new InMemoryDatasetDefinitionRegistry();
  }
}
