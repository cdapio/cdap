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

package io.cdap.cdap.data2.dataset2.lib.table;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.lib.CounterTimeseriesTable;
import io.cdap.cdap.api.dataset.lib.CounterTimeseriesTableDefinition;
import io.cdap.cdap.api.dataset.lib.IndexedObjectStore;
import io.cdap.cdap.api.dataset.lib.IndexedObjectStoreDefinition;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.lib.IndexedTableDefinition;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.KeyValueTableDefinition;
import io.cdap.cdap.api.dataset.lib.ObjectStore;
import io.cdap.cdap.api.dataset.lib.TimeseriesTable;
import io.cdap.cdap.api.dataset.lib.TimeseriesTableDefinition;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTable;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableDefinition;

/**
 * DatasetModule containing default datasets.
 *
 * Depends on {@link Table}.
 */
public class CoreDatasetsModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> tableDef = registry.get("table");

    DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = new KeyValueTableDefinition(KeyValueTable.TYPE,
                                                                                            tableDef);
    registry.add(kvTableDef);
    registry.add(new KeyValueTableDefinition(KeyValueTable.class.getName(), tableDef));

    DatasetDefinition<ObjectStore, DatasetAdmin> objectStoreDef = new ObjectStoreDefinition(ObjectStore.TYPE,
                                                                                            kvTableDef);
    registry.add(new ObjectStoreDefinition(ObjectStore.TYPE, kvTableDef));
    registry.add(new ObjectStoreDefinition(ObjectStore.class.getName(), kvTableDef));

    registry.add(new IndexedObjectStoreDefinition(IndexedObjectStore.TYPE, tableDef, objectStoreDef));
    registry.add(new IndexedObjectStoreDefinition(IndexedObjectStore.class.getName(), tableDef, objectStoreDef));

    registry.add(new IndexedTableDefinition(IndexedTable.TYPE, tableDef));
    registry.add(new IndexedTableDefinition(IndexedTable.class.getName(), tableDef));

    registry.add(new TimeseriesTableDefinition(TimeseriesTable.TYPE, tableDef));
    registry.add(new TimeseriesTableDefinition(TimeseriesTable.class.getName(), tableDef));

    registry.add(new CounterTimeseriesTableDefinition(CounterTimeseriesTable.TYPE, tableDef));
    registry.add(new CounterTimeseriesTableDefinition(CounterTimeseriesTable.class.getName(), tableDef));

    // in-memory table
    registry.add(new InMemoryTableDefinition(InMemoryTable.TYPE));
  }
}
