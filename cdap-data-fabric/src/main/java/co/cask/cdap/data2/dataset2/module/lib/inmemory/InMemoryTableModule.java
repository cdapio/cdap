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

package co.cask.cdap.data2.dataset2.module.lib.inmemory;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableDefinition;

/**
 * Registers in-memory implementations of the basic datasets
 */
public class InMemoryTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new InMemoryTableDefinition("table"));
    registry.add(new InMemoryTableDefinition(Table.class.getName()));
  }
}
