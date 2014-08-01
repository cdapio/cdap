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

package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.data2.dataset.api.DataSetManager;

import java.util.Properties;

/**
 *
 */
public class
  InMemoryOcTableManager implements DataSetManager {
  @Override
  public boolean exists(String name) {
    return InMemoryOcTableService.exists(name);
  }

  @Override
  public void create(String name) {
    InMemoryOcTableService.create(name);
  }

  @Override
  public void create(String name, @SuppressWarnings("unused") Properties props) throws Exception {
    create(name);
  }

  @Override
  public void truncate(String name) {
    InMemoryOcTableService.truncate(name);
  }

  @Override
  public void drop(String name) {
    InMemoryOcTableService.drop(name);
  }

  @Override
  public void upgrade(String name, Properties properties) throws Exception {
    // No-op
  }
}
