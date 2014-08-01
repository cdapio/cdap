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

package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Data set manager for hbase metrics tables. Implements TimeToLiveSupported as an indication of TTL.
 */
public class HBaseMetricsTableAdmin extends AbstractHBaseDataSetAdmin {

  public static final String PROPERTY_TTL = "ttl";
  private static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  private DatasetSpecification spec;

  @Inject
  public HBaseMetricsTableAdmin(DatasetSpecification spec,
                                Configuration hConf, HBaseTableUtil tableUtil) throws IOException {
    super(spec.getName(), new HBaseAdmin(hConf), tableUtil);
    this.spec = spec;
  }

  @Override
  public void create() throws IOException {
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
    tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
    columnDescriptor.setMaxVersions(1);

    int ttl = spec.getIntProperty(PROPERTY_TTL, -1);
    if (ttl > 0) {
      columnDescriptor.setTimeToLive(ttl);
    }

    final HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(columnDescriptor);
    tableUtil.createTableIfNotExists(admin, tableName, tableDescriptor);
  }

  @Override
  protected CoprocessorJar createCoprocessorJar() throws IOException {
    // No coprocessors for metrics table
    return CoprocessorJar.EMPTY;
  }

  @Override
  protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
    HColumnDescriptor columnDescriptor = tableDescriptor.getFamily(DATA_COLUMN_FAMILY);
    boolean needUpgrade = false;

    if (tableUtil.getBloomFilter(columnDescriptor) != HBaseTableUtil.BloomType.ROW) {
      tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
      needUpgrade = true;
    }
    if (columnDescriptor.getMaxVersions() != 1) {
      columnDescriptor.setMaxVersions(1);
      needUpgrade = true;
    }

    int ttl = spec.getIntProperty(PROPERTY_TTL, -1);
    if (ttl > 0 && columnDescriptor.getTimeToLive() != ttl) {
      columnDescriptor.setTimeToLive(ttl);
      needUpgrade = true;
    }

    return needUpgrade;
  }
}
