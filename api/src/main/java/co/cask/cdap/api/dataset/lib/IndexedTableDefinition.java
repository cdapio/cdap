/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Map;

/**
 * DatasetDefinition for {@link IndexedTable}.
 */
@Beta
public class IndexedTableDefinition
  extends AbstractDatasetDefinition<IndexedTable, DatasetAdmin> {
  
  private final DatasetDefinition<? extends Table, ?> tableDef;

  public IndexedTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("d", properties),
                tableDef.configure("i", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      tableDef.getAdmin(spec.getSpecification("d"), classLoader),
      tableDef.getAdmin(spec.getSpecification("i"), classLoader)
    ));
  }

  @Override
  public IndexedTable getDataset(DatasetSpecification spec,
                                 Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    DatasetSpecification tableInstance = spec.getSpecification("d");
    Table table = tableDef.getDataset(tableInstance, arguments, classLoader);

    DatasetSpecification indexTableInstance = spec.getSpecification("i");
    Table index = tableDef.getDataset(indexTableInstance, arguments, classLoader);

    String columnToIndex = spec.getProperty("columnToIndex");
    Preconditions.checkNotNull(columnToIndex, "columnToIndex must be specified");

    return new IndexedTable(spec.getName(), table, index, columnToIndex);
  }

}
