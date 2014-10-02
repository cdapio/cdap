/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.CounterTimeseriesTable;
import co.cask.cdap.api.dataset.lib.CounterTimeseriesTableDefinition;
import co.cask.cdap.api.dataset.lib.IndexedObjectStore;
import co.cask.cdap.api.dataset.lib.IndexedObjectStoreDefinition;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.IndexedTableDefinition;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.KeyValueTableDefinition;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.lib.TimeseriesTableDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.MemoryTable;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableDefinition;

/**
 * DatasetModule containing default datasets.
 *
 * Depends on {@link OrderedTable}.
 */
public class CoreDatasetsModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<OrderedTable, DatasetAdmin> orderedTableDef = registry.get("orderedTable", getVersion());

    DatasetDefinition<Table, DatasetAdmin> tableDef = new TableDefinition("table", getVersion(), orderedTableDef);
    registry.add(tableDef, getVersion());
    registry.add(new TableDefinition(Table.class.getName(), getVersion(), orderedTableDef), getVersion());

    DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef =
      new KeyValueTableDefinition("keyValueTable", getVersion(), tableDef);
    registry.add(kvTableDef, getVersion());
    registry.add(new KeyValueTableDefinition(KeyValueTable.class.getName(), getVersion(), tableDef), getVersion());

    DatasetDefinition<ObjectStore, DatasetAdmin> objectStoreDef = new
      ObjectStoreDefinition("objectStore", getVersion(), kvTableDef);
    registry.add(new ObjectStoreDefinition("objectStore", getVersion(), kvTableDef), getVersion());
    registry.add(new ObjectStoreDefinition(ObjectStore.class.getName(), getVersion(), kvTableDef), getVersion());

    registry.add(new IndexedObjectStoreDefinition("indexedObjectStore", getVersion(), tableDef, objectStoreDef),
                 getVersion());
    registry.add(new IndexedObjectStoreDefinition(
                   IndexedObjectStore.class.getName(), getVersion(), tableDef, objectStoreDef), getVersion());

    registry.add(new IndexedTableDefinition("indexedTable", getVersion(), tableDef), getVersion());
    registry.add(new IndexedTableDefinition(IndexedTable.class.getName(), getVersion(), tableDef), getVersion());

    registry.add(new TimeseriesTableDefinition("timeseriesTable", getVersion(), tableDef), getVersion());
    registry.add(new TimeseriesTableDefinition(TimeseriesTable.class.getName(), getVersion(), tableDef), getVersion());

    registry.add(new CounterTimeseriesTableDefinition("counterTimeseriesTable", getVersion(), tableDef), getVersion());
    registry.add(new CounterTimeseriesTableDefinition(CounterTimeseriesTable.class.getName(),
                                                      getVersion(), tableDef), getVersion());

    // in-memory table
    InMemoryOrderedTableDefinition inMemoryOrderedTable = new InMemoryOrderedTableDefinition("inMemoryOrderedTable",
                                                                                             getVersion());
    registry.add(new TableDefinition(MemoryTable.class.getName(), getVersion(), inMemoryOrderedTable), getVersion());
    registry.add(new TableDefinition("memoryTable", getVersion(), inMemoryOrderedTable), getVersion());
  }

  @Override
  public int getVersion() {
    return 0;
  }
}
