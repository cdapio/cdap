/*
 * Copyright 2016 Cask Data, Inc.
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
package co.cask.cdap.data2.dataset2.lib.cask360;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;
import co.cask.cdap.api.dataset.table.Table;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dataset definition of {@link Cask360Table}.
 */
public class Cask360TableDefinition extends AbstractDatasetDefinition<Cask360TableDataset, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  /**
   * Creates instance of {@link Cask360TableDefinition}.
   *
   * @param name
   *          this dataset type name
   * @param tableDef
   *          {@link Table} dataset definition, used to create tables to store
   *          360 data
   */
  public Cask360TableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    List<DatasetSpecification> datasetSpecs = Lists.newArrayList();

    datasetSpecs.add(tableDef.configure("data", properties));
    datasetSpecs.add(tableDef.configure("meta", properties));

    return DatasetSpecification.builder(instanceName, getName()).properties(properties.getProperties())
        .datasets(datasetSpecs).build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec, ClassLoader classLoader)
      throws IOException {
    Map<String, DatasetAdmin> admins = new HashMap<>();

    admins.put("data", tableDef.getAdmin(datasetContext, spec.getSpecification("data"), classLoader));
    admins.put("meta", tableDef.getAdmin(datasetContext, spec.getSpecification("meta"), classLoader));

    return new CompositeDatasetAdmin(admins);
  }

  @Override
  public Cask360TableDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
      Map<String, String> arguments, ClassLoader classLoader) throws IOException {

    Table data = tableDef.getDataset(datasetContext, spec.getSpecification("data"), arguments, classLoader);

    Table meta = tableDef.getDataset(datasetContext, spec.getSpecification("meta"), arguments, classLoader);

    return new Cask360TableDataset(spec, data, meta);
  }
}
