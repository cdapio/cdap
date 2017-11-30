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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * DatasetDefinition for {@link IndexedTable}.
 */
@Beta
public class IndexedTableDefinition
  extends CompositeDatasetDefinition<IndexedTable> {

  public IndexedTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name, "d", tableDef, "i", tableDef);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    String columnNamesToIndex = properties.getProperties().get(IndexedTable.INDEX_COLUMNS_CONF_KEY);
    if (columnNamesToIndex == null) {
      throw new IllegalArgumentException("columnsToIndex must be specified");
    }
    return super.configure(instanceName, properties);
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
                                          DatasetProperties newProperties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {

    // validate that the columns to index property is not null and the same as before
    String columnNamesToIndex = newProperties.getProperties().get(IndexedTable.INDEX_COLUMNS_CONF_KEY);
    if (columnNamesToIndex == null) {
      throw new IllegalArgumentException("columnsToIndex must be specified");
    }
    String oldColumnsToIndex = currentSpec.getProperty(IndexedTable.INDEX_COLUMNS_CONF_KEY);
    if (!columnNamesToIndex.equals(oldColumnsToIndex)) {
      Set<byte[]> newColumns = parseColumns(columnNamesToIndex);
      Set<byte[]> oldColumns = parseColumns(oldColumnsToIndex);
      if (!newColumns.equals(oldColumns)) {
        throw new IncompatibleUpdateException(String.format("Attempt to change columns to index from '%s' to '%s'",
                                                            oldColumnsToIndex, columnNamesToIndex));
      }
    }
    return super.reconfigure(instanceName, newProperties, currentSpec);
  }

  @Override
  public IndexedTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                 Map<String, String> arguments, ClassLoader classLoader) throws IOException {

    SortedSet<byte[]> columnsToIndex = parseColumns(spec.getProperty(IndexedTable.INDEX_COLUMNS_CONF_KEY));

    Table table = getDataset(datasetContext, "d", spec, arguments, classLoader);
    Table index = getDataset(datasetContext, "i", spec, arguments, classLoader);

    return new IndexedTable(spec.getName(), table, index, columnsToIndex);
  }

  /**
   * Helper method to parse a list of column names, comma-separated.
   */
  private SortedSet<byte[]> parseColumns(String value) {
    // TODO: add support for setting index key delimiter
    SortedSet<byte[]> columnsToIndex = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (String column : value.split(",")) {
      columnsToIndex.add(Bytes.toBytes(column));
    }
    return columnsToIndex;
  }
}
