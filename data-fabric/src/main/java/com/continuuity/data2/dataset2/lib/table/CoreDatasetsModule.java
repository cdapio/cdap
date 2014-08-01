/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.lib.IndexedObjectStore;
import com.continuuity.api.dataset.lib.IndexedObjectStoreDefinition;
import com.continuuity.api.dataset.lib.IndexedTable;
import com.continuuity.api.dataset.lib.IndexedTableDefinition;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.dataset.lib.KeyValueTableDefinition;
import com.continuuity.api.dataset.lib.MultiObjectStore;
import com.continuuity.api.dataset.lib.ObjectStore;
import com.continuuity.api.dataset.lib.TimeseriesTable;
import com.continuuity.api.dataset.lib.TimeseriesTableDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.MemoryTable;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableDefinition;

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
    registry.add(new ObjectStoreDefinition("objectStore", kvTableDef));
    registry.add(new ObjectStoreDefinition(ObjectStore.class.getName(), kvTableDef));

    registry.add(new IndexedObjectStoreDefinition("indexedObjectStore", tableDef, objectStoreDef));
    registry.add(new IndexedObjectStoreDefinition(IndexedObjectStore.class.getName(), tableDef, objectStoreDef));

    registry.add(new IndexedTableDefinition("indexedTable", tableDef));
    registry.add(new IndexedTableDefinition(IndexedTable.class.getName(), tableDef));

    registry.add(new MultiObjectStoreDefinition("multiObjectStore", tableDef));
    registry.add(new MultiObjectStoreDefinition(MultiObjectStore.class.getName(), tableDef));

    registry.add(new TimeseriesTableDefinition("timeseriesTable", tableDef));
    registry.add(new TimeseriesTableDefinition(TimeseriesTable.class.getName(), tableDef));

    // in-memory table
    InMemoryOrderedTableDefinition inMemoryOrderedTable = new InMemoryOrderedTableDefinition("inMemoryOrderedTable");
    registry.add(new TableDefinition(MemoryTable.class.getName(), inMemoryOrderedTable));
    registry.add(new TableDefinition("memoryTable", inMemoryOrderedTable));
  }

}
