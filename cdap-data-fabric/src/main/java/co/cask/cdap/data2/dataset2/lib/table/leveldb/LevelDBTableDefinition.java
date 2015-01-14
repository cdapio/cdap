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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class LevelDBTableDefinition
  extends AbstractDatasetDefinition<Table, LevelDBTableAdmin> {

  @Inject
  private LevelDBTableService service;

  public LevelDBTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public Table getDataset(DatasetSpecification spec,
                                        Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    return new LevelDBTable(spec.getName(), conflictDetection, service);
  }

  @Override
  public LevelDBTableAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new LevelDBTableAdmin(spec, service);
  }
}
