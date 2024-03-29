/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.test.app;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.AbstractDataset;
import io.cdap.cdap.api.dataset.lib.CompositeDatasetDefinition;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Increment;
import io.cdap.cdap.api.dataset.table.Table;
import java.io.IOException;
import java.util.Map;

public class UniqueCountTableDefinition
  extends CompositeDatasetDefinition<UniqueCountTableDefinition.UniqueCountTable> {

  public UniqueCountTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name, "entryCountTable", tableDef, "uniqueCountTable", tableDef);
  }

  @Override
  public UniqueCountTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                     Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    return new UniqueCountTable(spec.getName(),
                                this.<Table>getDataset(datasetContext, "entryCountTable",
                                                       spec, arguments, classLoader),
                                this.<Table>getDataset(datasetContext, "uniqueCountTable",
                                                       spec, arguments, classLoader));
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
      long newCount = entryCountTable.incrementAndGet(new Increment(entry, "count", 1L)).getInt("count");
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

