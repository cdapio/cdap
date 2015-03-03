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

package co.cask.cdap.data2.dataset2.lib.table.leveldb;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Map;

/**
 * LevelDB backed implementation for {@link co.cask.cdap.data2.dataset2.lib.table.MetricsTable}
 */
public class LevelDBMetricsTableDefinition
  extends AbstractDatasetDefinition<MetricsTable, DatasetAdmin> {

  @Inject
  private LevelDBTableService service;

  public LevelDBMetricsTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public MetricsTable getDataset(DatasetContext datasetContext, Map<String, String> arguments, ClassLoader classLoader,
                                 DatasetSpecification spec) throws IOException {
    return new LevelDBMetricsTable(spec.getName(), service);
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, ClassLoader classLoader,
                               DatasetSpecification spec) throws IOException {
    // the table management is the same as in ordered table
    return new LevelDBTableAdmin(spec, service);
  }
}
