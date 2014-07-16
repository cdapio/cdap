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

package com.continuuity.data2.dataset.lib.table;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.api.DataSetManager;

import java.util.Properties;

/**
 * Common utility for managing system metadata tables needed by various services.
 */
public abstract class MetaTableUtil {

  protected final DataSetAccessor dataSetAccessor;

  public MetaTableUtil(DataSetAccessor dataSetAccessor) {
    this.dataSetAccessor = dataSetAccessor;
  }

  public OrderedColumnarTable getMetaTable() throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    String tableName = getMetaTableName();
    if (!dsManager.exists(tableName)) {
      dsManager.create(tableName);
    }

    return dataSetAccessor.getDataSetClient(tableName,
                                            OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM);
  }

  public void upgrade() throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    dsManager.upgrade(getMetaTableName(), new Properties());
  }

  /**
   * Returns the name of the metadata table to be used.
   */
  public abstract String getMetaTableName();
}
