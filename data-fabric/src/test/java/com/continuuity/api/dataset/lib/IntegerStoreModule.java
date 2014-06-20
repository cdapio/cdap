package com.continuuity.api.dataset.lib;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;

/**
 *
 */
public class IntegerStoreModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = registry.get("keyValueTable");
    IntegerStoreDefinition definition = new IntegerStoreDefinition("integerStore", kvTableDef);
    registry.add(definition);
  }
}
