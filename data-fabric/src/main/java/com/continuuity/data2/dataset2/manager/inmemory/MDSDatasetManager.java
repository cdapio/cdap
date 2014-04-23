package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.google.inject.Inject;

/**
 *
 */
public class MDSDatasetManager extends InMemoryDatasetManager {
  @Inject
  public MDSDatasetManager(DatasetDefinitionRegistry registry) {
    super(registry);
  }
}
