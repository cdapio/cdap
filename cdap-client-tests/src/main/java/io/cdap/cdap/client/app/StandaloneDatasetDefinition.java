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

package io.cdap.cdap.client.app;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.AbstractDatasetDefinition;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class StandaloneDatasetDefinition extends
    AbstractDatasetDefinition<StandaloneDataset, DatasetAdmin> {

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public StandaloneDatasetDefinition(String name,
      DatasetDefinition<? extends KeyValueTable, ?> keyValueDef) {
    super(name);
    Preconditions.checkArgument(keyValueDef != null, "KeyValueTable definition is required");
    this.tableDef = keyValueDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
        .properties(properties.getProperties())
        .datasets(tableDef.configure("objects", properties))
        .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
      ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(datasetContext, spec.getSpecification("objects"), classLoader);
  }

  @Override
  public StandaloneDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
      Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("objects");
    KeyValueTable table = tableDef.getDataset(datasetContext, kvTableSpec, arguments, classLoader);

    return new StandaloneDataset(spec.getName(), table);
  }
}
