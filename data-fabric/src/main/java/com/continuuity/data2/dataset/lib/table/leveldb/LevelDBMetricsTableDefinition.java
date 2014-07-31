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

package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableAdmin;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * LevelDB backed implementation for {@link com.continuuity.data2.dataset.lib.table.MetricsTable}
 */
public class LevelDBMetricsTableDefinition
  extends AbstractDatasetDefinition<MetricsTable, DatasetAdmin> {

  @Inject
  private LevelDBOcTableService service;

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
  public MetricsTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new LevelDBMetricsTable(spec.getName(), service);
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    // the table management is the same as in ordered table
    return new LevelDBOrderedTableAdmin(spec, service);
  }
}
