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

package com.continuuity.test.app;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.lib.CompositeDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Increment;
import com.continuuity.api.dataset.table.Table;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;

public class UniqueCountTableDefinition
  extends CompositeDatasetDefinition<UniqueCountTableDefinition.UniqueCountTable> {

  public UniqueCountTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name, ImmutableMap.of("entryCountTable", tableDef,
                                "uniqueCountTable", tableDef));
  }

  @Override
  public UniqueCountTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new UniqueCountTable(spec.getName(),
                                getDataset("entryCountTable", Table.class, spec, classLoader),
                                getDataset("uniqueCountTable", Table.class, spec, classLoader));
  }

  public static class UniqueCountTable extends AbstractDataset {

    private final Table entryCountTable;
    private final Table uniqueCountTable;

    public UniqueCountTable(String instanceName,
                            Table entryCountTable,
                            Table uniqueCountTable) {
      super(instanceName, entryCountTable, uniqueCountTable);
      this.entryCountTable = entryCountTable;
      this.uniqueCountTable = uniqueCountTable;
    }

    public void updateUniqueCount(String entry) {
      long newCount = entryCountTable.increment(new Increment(entry, "count", 1L)).getInt("count");
      if (newCount == 1L) {
        uniqueCountTable.increment(new Increment("unique_count", "count", 1L));
      }
    }

    public Long readUniqueCount() {
      return uniqueCountTable.get(new Get("unique_count", "count"))
        .getLong("count", 0);
    }

  }

  /**
   * Dataset module
   */
  public static class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> tableDefinition = registry.get("table");
      UniqueCountTableDefinition keyValueTable = new UniqueCountTableDefinition("uniqueCountTable", tableDefinition);
      registry.add(keyValueTable);
    }
  }
}

