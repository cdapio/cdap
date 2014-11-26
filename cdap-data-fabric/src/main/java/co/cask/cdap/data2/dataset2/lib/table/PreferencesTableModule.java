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
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.OrderedTable;

/**
 * {@link DatasetModule} for {@link PreferencesTable}
 */
public class PreferencesTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<OrderedTable, DatasetAdmin> tableDefinition = registry.get(OrderedTable.class.getName());
    registry.add(new PreferencesTableDefinition("prefTable", tableDefinition));
    registry.add(new PreferencesTableDefinition(PreferencesTable.class.getName(), tableDefinition));
  }
}
