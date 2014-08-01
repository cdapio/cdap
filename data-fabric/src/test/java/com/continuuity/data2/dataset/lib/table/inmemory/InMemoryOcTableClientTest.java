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
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;

/**
 *
 */
public class InMemoryOcTableClientTest extends BufferingOcTableClientTest<InMemoryOcTableClient> {
  @Override
  protected InMemoryOcTableClient getTable(String name, ConflictDetection level) {
    return new InMemoryOcTableClient(name, level);
  }

  @Override
  protected DataSetManager getTableManager() {
    return new InMemoryOcTableManager();
  }
}
