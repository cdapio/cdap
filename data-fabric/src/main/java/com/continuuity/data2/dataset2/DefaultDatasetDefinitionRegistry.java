package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.DatasetDefinition;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * this is a hack for initializing system-level datasets for now
 */
public class DefaultDatasetDefinitionRegistry extends InMemoryDatasetDefinitionRegistry {
  @Inject
  private Injector injector;

  @Override
  public void add(DatasetDefinition def) {
    injector.injectMembers(def);
    super.add(def);
  }
}
