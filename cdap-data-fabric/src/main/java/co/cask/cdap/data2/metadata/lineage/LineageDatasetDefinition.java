/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.Map;

/**
 * {@link co.cask.cdap.api.dataset.DatasetDefinition} for {@link LineageDataset}.
 */
public class LineageDatasetDefinition extends AbstractDatasetDefinition<LineageDataset, DatasetAdmin> {
  public static final String ACCESS_REGISTRY_TABLE = "access_registry";

  private final DatasetDefinition<Table, ? extends DatasetAdmin> tableDefinition;

  public LineageDatasetDefinition(String name, DatasetDefinition<Table, ? extends DatasetAdmin> tableDefinition) {
    super(name);
    this.tableDefinition = tableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    // Use ConflictDetection.NONE as we only need a flag whether a program uses a dataset/stream.
    // Having conflict detection will lead to failures when programs try to register accesses at the same time.
    DatasetProperties datasetProperties = DatasetProperties.builder()
      .addAll(properties.getProperties())
      .add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.NONE.name())
      .build();

    return DatasetSpecification.builder(instanceName, getName())
      .properties(datasetProperties.getProperties())
      .datasets(tableDefinition.configure(ACCESS_REGISTRY_TABLE, datasetProperties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec, ClassLoader classLoader)
    throws IOException {
    return tableDefinition.getAdmin(datasetContext, spec.getSpecification(ACCESS_REGISTRY_TABLE), classLoader);
  }

  @Override
  public LineageDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                   Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table =
      tableDefinition.getDataset(datasetContext, spec.getSpecification(ACCESS_REGISTRY_TABLE), arguments, classLoader);
    return new LineageDataset(spec.getName(), table);
  }
}
