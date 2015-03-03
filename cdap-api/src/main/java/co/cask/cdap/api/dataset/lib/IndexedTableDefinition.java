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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
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

  /**
   * Configuration key for defining column names to index in the DatasetSpecification properties.
   * Multiple column names should be listed as a comma-separated string, e.g. "column1,column2,etc".
   */
  public static final String INDEX_COLUMNS_CONF_KEY = "columnsToIndex";

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
  public DatasetAdmin getAdmin(DatasetContext datasetContext, ClassLoader classLoader,
                               DatasetSpecification spec) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      tableDef.getAdmin(datasetContext, classLoader, spec.getSpecification("d")),
      tableDef.getAdmin(datasetContext, classLoader, spec.getSpecification("i"))
    ));
  }

  @Override
  public IndexedTable getDataset(DatasetContext datasetContext, Map<String, String> arguments, ClassLoader classLoader,
                                 DatasetSpecification spec) throws IOException {
    DatasetSpecification tableInstance = spec.getSpecification("d");
    Table table = tableDef.getDataset(datasetContext, arguments, classLoader, tableInstance);

    DatasetSpecification indexTableInstance = spec.getSpecification("i");
    Table index = tableDef.getDataset(datasetContext, arguments, classLoader, indexTableInstance);

    String columnNamesToIndex = spec.getProperty(INDEX_COLUMNS_CONF_KEY);
    Preconditions.checkNotNull(columnNamesToIndex, "columnsToIndex must be specified");
    String[] columns = columnNamesToIndex.split(",");
    byte[][] columnsToIndex = new byte[columns.length][];
    for (int i = 0; i < columns.length; i++) {
      columnsToIndex[i] = Bytes.toBytes(columns[i]);
    }

    // TODO: add support for setting index key delimiter

    return new IndexedTable(spec.getName(), table, index, columnsToIndex);
  }

}
