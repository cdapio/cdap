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
 * DatasetDefinition for {@link IndexedObjectStore}.
 */
@Beta
public class IndexedObjectStoreDefinition
  extends AbstractDatasetDefinition<IndexedObjectStore, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;
  private final DatasetDefinition<? extends ObjectStore, ?> objectStoreDef;

  public IndexedObjectStoreDefinition(String name,
                                      DatasetDefinition<? extends Table, ?> tableDef,
                                      DatasetDefinition<? extends ObjectStore, ?> objectStoreDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    Preconditions.checkArgument(objectStoreDef != null, "ObjectStore definition is required");
    this.tableDef = tableDef;
    this.objectStoreDef = objectStoreDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("index", properties),
                objectStoreDef.configure("data", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      tableDef.getAdmin(datasetContext, spec.getSpecification("index"), classLoader),
      objectStoreDef.getAdmin(datasetContext, spec.getSpecification("data"), classLoader)
    ));
  }

  @Override
  public IndexedObjectStore<?> getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                          ClassLoader classLoader, Map<String, String> arguments) throws IOException {
    DatasetSpecification tableSpec = spec.getSpecification("index");
    DatasetSpecification objectStoreSpec = spec.getSpecification("data");

    Table index = tableDef.getDataset(datasetContext, tableSpec, classLoader, arguments);
    ObjectStore<?> objectStore = objectStoreDef.getDataset(datasetContext, objectStoreSpec, classLoader, arguments);

    return new IndexedObjectStore(spec.getName(), objectStore, index);
  }

}
