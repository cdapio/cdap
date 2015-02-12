/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.Map;

/**
 * Defines Table data set
 */
@Deprecated
public class OrderedTableDefinition extends AbstractDatasetDefinition<OrderedTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public OrderedTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
    super(name);
    this.tableDef = tableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification(""), classLoader);
  }

  @Override
  public OrderedTable getDataset(DatasetSpecification spec, Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    Table table = tableDef.getDataset(spec.getSpecification(""), arguments, classLoader);
    return new OrderedTableDataset(spec.getName(), table);
  }
}
