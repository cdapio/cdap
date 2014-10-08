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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.api.dataset.DatasetAdmin;

import java.io.IOException;

/**
 *
 */
public class InMemoryOrderedTableAdmin implements DatasetAdmin {
  private final String name;

  public InMemoryOrderedTableAdmin(String name) {
    this.name = name;
  }

  @Override
  public boolean exists() {
    return InMemoryOrderedTableService.exists(name);
  }

  @Override
  public void create() {
    InMemoryOrderedTableService.create(name);
  }

  @Override
  public void truncate() {
    InMemoryOrderedTableService.truncate(name);
  }

  @Override
  public void drop() {
    InMemoryOrderedTableService.drop(name);
  }

  @Override
  public void upgrade() {
    // no-op
  }

  @Override
  public void close() throws IOException {
    // NOTHING to do
  }
}
