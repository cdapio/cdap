package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Table;

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
    registry.add(new TableDefinition(Table.class.getName(), orderedTableDef));

    DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = new KeyValueTableDefinition("keyValueTable", tableDef);
    registry.add(kvTableDef);
    registry.add(new KeyValueTableDefinition(KeyValueTable.class.getName(), tableDef));

    DatasetDefinition<ObjectStore, DatasetAdmin> objectStoreDef = new ObjectStoreDefinition("objectStore", kvTableDef);
    registry.add(objectStoreDef);
    registry.add(new ObjectStoreDefinition(ObjectStore.class.getName(), kvTableDef));

    registry.add(new IndexedObjectStoreDefinition("indexedObjectStore", tableDef, objectStoreDef));
    registry.add(new IndexedObjectStoreDefinition(IndexedObjectStore.class.getName(), tableDef, objectStoreDef));

    registry.add(new IndexedTableDefinition("indexedTable", tableDef));
    registry.add(new IndexedTableDefinition(IndexedTable.class.getName(), tableDef));

    registry.add(new MultiObjectStoreDefinition("multiObjectStore", tableDef));
    registry.add(new MultiObjectStoreDefinition(MultiObjectStore.class.getName(), tableDef));
  }

}
