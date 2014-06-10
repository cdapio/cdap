package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * {@link DatasetModule} for {@link KeyValueTable}.
 */
public class KeyValueTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> tableDef = registry.get("table");
    KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("keyValueTable", tableDef);
    registry.add(keyValueTable);
  }
}
