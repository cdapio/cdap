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

package co.cask.cdap.data2.dataset2.cache;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

import java.io.IOException;
import java.util.Map;

public class TestDatasetDefinition extends AbstractDatasetDefinition<TestDataset, DatasetAdmin> {

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public TestDatasetDefinition(
    String name, DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef) {
    super(name);
    this.tableDef = kvTableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("kv", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(datasetContext, spec.getSpecification("kv"), classLoader);
  }

  @Override
  public TestDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("kv");
    KeyValueTable table = tableDef.getDataset(
      datasetContext, kvTableSpec, DatasetDefinition.NO_ARGUMENTS, classLoader);
    return new TestDataset(spec, table, arguments);
  }
}
