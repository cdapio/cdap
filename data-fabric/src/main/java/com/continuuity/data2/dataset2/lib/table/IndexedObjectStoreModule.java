package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * DatasetModule for {@link com.continuuity.data2.dataset2.lib.table.ObjectStore}.
 *
 * @param <T> Type of object that the {@link com.continuuity.data2.dataset2.lib.table.ObjectStore} will store.
 */
public class IndexedObjectStoreModule<T> implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> tableDef = registry.get("table");
    DatasetDefinition<ObjectStore<T>, DatasetAdmin> objectStoreDef = registry.get("objectStore");
    registry.add(new IndexedObjectStoreDefinition<T>("indexedObjectStore", tableDef, objectStoreDef));
  }
}
