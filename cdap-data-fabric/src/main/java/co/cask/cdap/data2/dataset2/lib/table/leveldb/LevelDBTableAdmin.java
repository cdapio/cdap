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
import co.cask.cdap.api.dataset.DatasetSpecification;

import java.io.IOException;

/**
 *
 */
public class LevelDBTableAdmin implements DatasetAdmin {

  private final LevelDBTableService service;
  private final String name;

  public LevelDBTableAdmin(DatasetSpecification spec, LevelDBTableService service) throws IOException {
    this.service = service;
    this.name = spec.getName();
  }

  @Override
  public boolean exists() throws IOException {
    try {
      service.getTable(name);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void create() throws IOException {
    service.ensureTableExists(name);
  }

  @Override
  public void drop() throws IOException {
    service.dropTable(name);
  }

  @Override
  public void truncate() throws IOException {
    drop();
    create();
  }

  @Override
  public void upgrade() throws IOException {
    // no-op
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
