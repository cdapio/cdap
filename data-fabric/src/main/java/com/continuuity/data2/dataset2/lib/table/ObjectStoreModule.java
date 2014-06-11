package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * DatasetModule for {@link ObjectStore}.
 *
 * @param <T> Type of object that the {@link ObjectStore} will store.
 */
public class ObjectStoreModule<T> implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<KeyValueTable, DatasetAdmin> objectStore = registry.get("keyValueTable");
    ObjectStoreDefinition definition = new ObjectStoreDefinition<T>("objectStore", objectStore);
    registry.add(definition);
  }
}
