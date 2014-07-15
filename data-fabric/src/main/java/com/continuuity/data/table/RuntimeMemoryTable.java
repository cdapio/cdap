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

package com.continuuity.data.table;

import com.continuuity.data.DataFabric;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableManager;
import com.google.common.base.Throwables;

/**
 * Base class for runtime implementations of MemoryTable.
 */
public class RuntimeMemoryTable extends RuntimeTable {
  /**
   * Creates instance of Runtime Table. This constructor is called by DataSetInstantiator at runtime only,
   * hence the table name doesn't matter, as it'll get initialized
   * in the {@link #initialize(com.continuuity.api.data.DataSetSpecification, com.continuuity.api.data.DataSetContext)}
   * method.
   */
  public RuntimeMemoryTable(DataFabric dataFabric, String metricName) {
    super(dataFabric, metricName);
  }

  @Override
  protected OrderedColumnarTable getOcTable(DataFabric dataFabric) {
    DataSetManager dataSetManager = dataFabric.getDataSetManager(InMemoryOcTableManager.class);

    try {
      // We want to ensure table is there before creating table client
      if (!dataSetManager.exists(getName())) {
        dataSetManager.create(getName());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return dataFabric.getDataSetClient(getName(), InMemoryOcTableClient.class, null);
  }
}
