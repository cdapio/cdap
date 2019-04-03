/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.replication;

import co.cask.cdap.replication.ReplicationConstants;
import co.cask.cdap.replication.StatusUtils;
import co.cask.cdap.replication.TableUpdater;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Common functionality required by Replication State Coprocessors to hold updates in memory and
 * flush into HBase periodically.
 */
public class HBase11TableUpdater extends TableUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(HBase11TableUpdater.class);
  private final HTableInterface hTableInterface;

  public HBase11TableUpdater(String rowType, Configuration conf, HTableInterface hTableInterface) {
    super(rowType, conf);
    this.hTableInterface = hTableInterface;
  }

  @Override
  protected void writeState(Map<String, Long> cachedUpdates) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (Map.Entry<String, Long> entry : cachedUpdates.entrySet()) {
      Put put = new Put(getRowKey(entry.getKey()));
      put.addColumn(columnFamily,
                    Bytes.toBytes(rowType),
                    Bytes.toBytes(entry.getValue()));
      puts.add(put);
    }

    if (!puts.isEmpty()) {
      LOG.debug("Update Replication State table now. {} entries.", puts.size());
      hTableInterface.put(puts);
    }
  }

  @Override
  protected void createTableIfNotExists(Configuration conf) throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      String tableName = StatusUtils.getReplicationStateTableName(conf);
      if (admin.tableExists(tableName)) {
        return;
      }
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
      htd.addFamily(new HColumnDescriptor(ReplicationConstants.ReplicationStatusTool.TIME_FAMILY));
      admin.createTable(htd);
      LOG.info("Created Table {}.", tableName);
    }
  }
}
