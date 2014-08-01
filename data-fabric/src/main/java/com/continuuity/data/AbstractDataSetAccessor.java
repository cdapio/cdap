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
import com.continuuity.tephra.TxConstants;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * TODO: Having this class is a bad design: we want to add more dataset types to a system without changing core code.
 *       We need to review that during creating dataset management service
 */
public abstract class AbstractDataSetAccessor extends NamespacingDataSetAccessor {
  protected AbstractDataSetAccessor(CConfiguration conf) {
    super(conf);
  }

  protected abstract <T> T getOcTableClient(String name, ConflictDetection level, int ttl) throws Exception;
  protected abstract DataSetManager getOcTableManager() throws Exception;

  @SuppressWarnings("unchecked")
  @Override
  protected  final <T> T getDataSetClient(String name,
                                          Class<? extends T> type,
                                          @Nullable Properties props) throws Exception {
    // This is work-around for getting always in-memory ocTable. Will be fixed when we have per-dataset configuration
    if (type == InMemoryOcTableClient.class) {
      // no need to do conflict detection for pure in-memroy dataset
      return (T) new InMemoryOcTableClient(name, ConflictDetection.NONE);

    } else if (type == OrderedColumnarTable.class) {
      ConflictDetection level = null;
      int ttl = -1;
      if (props != null) {
        String levelProperty = props.getProperty("conflict.level");
        level = levelProperty == null ? null : ConflictDetection.valueOf(levelProperty);
        String ttlProperty = props.getProperty(TxConstants.PROPERTY_TTL);
        ttl = ttlProperty == null ? -1 : Integer.valueOf(ttlProperty);
      }
      // using ROW by default
      level = level == null ? ConflictDetection.ROW : level;
      return getOcTableClient(name, level, ttl);
    }

    return null;
  }

  @Override
  protected final DataSetManager getDataSetManager(Class type) throws Exception {
    // This is work-around for getting always in-memory ocTable. Will be fixed when we have per-dataset configuration
    if (type == InMemoryOcTableManager.class) {
      return new InMemoryOcTableManager();

    } else if (type == OrderedColumnarTable.class) {
      return getOcTableManager();
    }
    return null;
  }

}
