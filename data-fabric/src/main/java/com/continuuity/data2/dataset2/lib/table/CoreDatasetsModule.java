package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * DatasetModule containing default datasets.
 *
 * Depends on {@link OrderedTable}.
 */
public class CoreDatasetsModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<OrderedTable, DatasetAdmin> orderedTableDef = registry.get("orderedTable");

    DatasetDefinition<Table, DatasetAdmin> tableDef = new TableDefinition("table", orderedTableDef);
    registry.add(tableDef);

    DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = new KeyValueTableDefinition("keyValueTable", tableDef);
    registry.add(kvTableDef);

    DatasetDefinition<ObjectStore, DatasetAdmin> objectStoreDef = new ObjectStoreDefinition("objectStore", kvTableDef);
    registry.add(objectStoreDef);

    registry.add(new IndexedObjectStoreDefinition("indexedObjectStore", tableDef, objectStoreDef));
    registry.add(new IndexedTableDefinition("indexedTable", tableDef));
    registry.add(new MultiObjectStoreDefinition("multiObjectStore", tableDef));
  }

}
