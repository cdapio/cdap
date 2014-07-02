package com.continuuity.data2.dataset2;

import com.continuuity.data2.datafabric.dataset.InMemoryDefinitionRegistryFactory;

/**
 *
 */
public class InMemoryDatasetFrameworkTest extends AbstractDatasetFrameworkTest {
  @Override
  protected DatasetFramework getFramework() {
    return new InMemoryDatasetFramework(new InMemoryDefinitionRegistryFactory());
  }
}
