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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.ACLTable;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.IndexedObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.security.ACL;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Map;

/**
 * {@link DatasetDefinition} for {@link ACLTable}.
 */
public class ACLTableDefinition extends AbstractDatasetDefinition<ACLTable, DatasetAdmin> {

  private final DatasetDefinition<? extends IndexedObjectStore, ?> tableDefinition;

  public ACLTableDefinition(String name, DatasetDefinition<? extends IndexedObjectStore, ?> tableDefinition) {
    super(name);
    this.tableDefinition = tableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDefinition.configure("data", getObjectStoreProperties()))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDefinition.getAdmin(spec.getSpecification("data"), classLoader);
  }

  @Override
  public ACLTable getDataset(DatasetSpecification spec, Map<String, String> arguments,
                             ClassLoader classLoader) throws IOException {
    DatasetSpecification tableSpec = spec.getSpecification("data");
    IndexedObjectStore<ACL> data = tableDefinition.getDataset(tableSpec, arguments, classLoader);
    return new ACLTableDataset(spec, data);
  }

  private DatasetProperties getObjectStoreProperties() {
    try {
      return ObjectStores.objectStoreProperties(ACL.class, DatasetProperties.EMPTY);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }
}
