/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.dataset.lib;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.dataset2.lib.table.ObjectStoreDataset;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.io.IOException;

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
  public ObjectStoreDataset<Integer> getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("table");
    KeyValueTable table = tableDef.getDataset(kvTableSpec, classLoader);

    try {
      return new IntegerStore(spec.getName(), table);
    } catch (UnsupportedTypeException e) {
      // shouldn't happen
      throw new IOException(e);
    }
  }

}
