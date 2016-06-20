/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.registry;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.Reconfigurable;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.Map;

/**
 * The Dataset Module for defining the {@link UsageDataset}.
 */
public class UsageDatasetModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, DatasetAdmin> tableDef = registry.get("table");
    registry.add(new UsageDatasetDefinition(UsageDataset.class.getSimpleName(), tableDef));
  }

  /**
   * Dataset definition for creating {@link UsageDataset}.
   */
  public static final class UsageDatasetDefinition
    extends AbstractDatasetDefinition<UsageDataset, DatasetAdmin>
    implements Reconfigurable {

    private final DatasetDefinition<? extends Table, ?> tableDefinition;

    protected UsageDatasetDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
      super(name);
      this.tableDefinition = tableDefinition;
    }

    @Override
    public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
      // Use ConflictDetection.NONE as we only need a flag whether a program uses a dataset/stream.
      // Having conflict detection will lead to failures when programs start, and all try to register at the same time.
      DatasetProperties datasetProperties = DatasetProperties.builder()
        .addAll(properties.getProperties())
        .add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.NONE.name())
        .build();
      DatasetSpecification spec = tableDefinition.configure(instanceName, datasetProperties);
      return DatasetSpecification.builder(instanceName, getName())
        .properties(spec.getProperties())
        .build();
    }

    @Override
    public DatasetSpecification reconfigure(String instanceName, DatasetProperties properties,
                                            DatasetSpecification currentSpec) throws IncompatibleUpdateException {
      // Use ConflictDetection.NONE as we only need a flag whether a program uses a dataset/stream.
      // Having conflict detection will lead to failures when programs start, and all try to register at the same time.
      DatasetProperties datasetProperties = DatasetProperties.builder()
        .addAll(properties.getProperties())
        .add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.NONE.name())
        .build();
      DatasetSpecification spec = AbstractDatasetDefinition
        .reconfigure(tableDefinition, instanceName, datasetProperties, currentSpec);
      return DatasetSpecification.builder(instanceName, getName())
        .properties(spec.getProperties())
        .build();
    }

    @Override
    public DatasetAdmin getAdmin(DatasetContext datasetContext,
                                 DatasetSpecification spec, ClassLoader classLoader) throws IOException {
      return tableDefinition.getAdmin(datasetContext, spec, classLoader);
    }

    @Override
    public UsageDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                   Map<String, String> arguments, ClassLoader classLoader) throws IOException {
      Table table = tableDefinition.getDataset(datasetContext, spec, arguments, classLoader);
      return new UsageDataset(table);
    }
  }
}
