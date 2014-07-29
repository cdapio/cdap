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

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableManager;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DistributedDataSetAccessor extends AbstractDataSetAccessor {
  protected final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final HBaseTableUtil tableUtil;

  @Inject
  public DistributedDataSetAccessor(CConfiguration cConf,
                                    Configuration hConf,
                                    LocationFactory locationFactory,
                                    HBaseTableUtil tableUtil)
    throws IOException {
    super(cConf);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.tableUtil = tableUtil;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T getOcTableClient(String name, ConflictDetection level, int ttl) throws Exception {
    return (T) new HBaseOcTableClient(name, level, ttl, hConf);
  }

  @Override
  protected DataSetManager getOcTableManager() throws Exception {
    return new HBaseOcTableManager(cConf, hConf, locationFactory, tableUtil);
  }

  @Override
  protected Map<String, Class<?>> list(String prefix) throws Exception {
    Map<String, Class<?>> datasets = Maps.newHashMap();
    HBaseAdmin admin = new HBaseAdmin(hConf);
    for (HTableDescriptor tableDescriptor : admin.listTables()) {
      String tableName = Bytes.toString(tableDescriptor.getName());
      if (tableName.startsWith(prefix)) {
        datasets.put(tableName, OrderedColumnarTable.class);
      }
    }
    return datasets;
  }
}

