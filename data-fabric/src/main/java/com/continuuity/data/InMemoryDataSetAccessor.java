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

package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableManager;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableService;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.Map;

/**
 * Provides access to datasets in in-memory mode.
 */
public class InMemoryDataSetAccessor extends AbstractDataSetAccessor {
  @Inject
  public InMemoryDataSetAccessor(CConfiguration conf) {
    super(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T getOcTableClient(String name, ConflictDetection level, int ttl) throws Exception {
    // ttl is ignored in local mode
    return (T) new InMemoryOcTableClient(name, level);
  }

  @Override
  protected DataSetManager getOcTableManager() throws Exception {
    return new InMemoryOcTableManager();
  }

  @Override
  protected Map<String, Class<?>> list(String prefix) throws Exception {
    Map<String, Class<?>> datasets = Maps.newHashMap();
    for (String tableName : InMemoryOcTableService.list()) {
      if (tableName.startsWith(prefix)) {
        datasets.put(tableName, OrderedColumnarTable.class);
      }
    }
    return datasets;
  }
}

