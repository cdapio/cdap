package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * DatasetModule for {@link com.continuuity.data2.dataset2.lib.table.MultiObjectStore}.
 *
 * @param <T> Type of object that the {@link com.continuuity.data2.dataset2.lib.table.MultiObjectStore} will store.
 */
public class MultiObjectStoreModule<T> implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> tableDef = registry.get("table");
    registry.add(new MultiObjectStoreDefinition<T>("multiObjectStore", tableDef));
  }
}
