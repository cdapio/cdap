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

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.data2.dataset2.lib.table.ObjectStoreDataset;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class IntegerStoreDefinition
  extends AbstractDatasetDefinition<ObjectStoreDataset<Integer>, DatasetAdmin> {

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public IntegerStoreDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> keyValueTableDefinition) {
    super(name);
    this.tableDef = keyValueTableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("table"), classLoader);
  }

  @Override
  public ObjectStoreDataset<Integer> getDataset(DatasetSpecification spec,
                                                Map<String, String> arguments,
                                                ClassLoader classLoader) throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("table");
    KeyValueTable table = tableDef.getDataset(kvTableSpec, arguments, classLoader);

    try {
      return new IntegerStore(spec.getName(), table);
    } catch (UnsupportedTypeException e) {
      // shouldn't happen
      throw new IOException(e);
    }
  }

}
