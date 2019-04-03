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

package io.cdap.cdap.data2.dataset2.lib.table;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition;

/**
 * {@link io.cdap.cdap.api.dataset.module.DatasetModule} for {@link io.cdap.cdap.api.dataset.lib.cube.Cube}.
 */
public class CubeModule implements DatasetModule {
  public static final String SHORT_NAME = Cube.TYPE;
  public static final String FULL_NAME = Cube.class.getName();

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, ? extends DatasetAdmin> tableDef = registry.get(Table.TYPE);
    DatasetDefinition<MetricsTable, ? extends DatasetAdmin> metricsTableDef =
      registry.get(MetricsTable.class.getName());
    registry.add(new CubeDatasetDefinition(FULL_NAME, tableDef, metricsTableDef));
    registry.add(new CubeDatasetDefinition(SHORT_NAME, tableDef, metricsTableDef));
  }
}
