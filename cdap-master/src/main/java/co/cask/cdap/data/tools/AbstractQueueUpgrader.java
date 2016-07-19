/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * Upgrades row keys of a queue table
 */
public abstract class AbstractQueueUpgrader extends AbstractUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractQueueUpgrader.class);
  protected final HBaseTableUtil tableUtil;
  protected final Configuration conf;
  protected final NamespaceQueryAdmin namespaceQueryAdmin;

  protected AbstractQueueUpgrader(LocationFactory locationFactory,
                                  NamespacedLocationFactory namespacedLocationFactory,
                                  HBaseTableUtil tableUtil, Configuration conf,
                                  NamespaceQueryAdmin namespaceQueryAdmin) {
    super(locationFactory, namespacedLocationFactory);
    this.tableUtil = tableUtil;
    this.conf = conf;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * @return Iterable of TableId of the tables to upgrade
   */
  protected abstract Iterable<TableId> getTableIds() throws Exception;

  /**
   * @param oldRowKey the old row key of the row to migrate
   * @return row key for the row to migrate, or null if the row is not to be migrated
   */
  @Nullable
  protected abstract byte[] processRowKey(byte[] oldRowKey);

  @Override
  void upgrade() throws Exception {
    Iterable<TableId> tableIds = getTableIds();
    for (TableId tableId : tableIds) {
      LOG.info("Upgrading table {}", tableId);

      if (!tableUtil.tableExists(new HBaseAdmin(conf), tableId)) {
        LOG.info("Table does not exist: {}. No upgrade necessary.", tableId);
        continue;
      }
      HTable hTable = tableUtil.createHTable(conf, tableId);
      ProjectInfo.Version tableVersion = HBaseTableUtil.getVersion(hTable.getTableDescriptor());
      // Only upgrade if Upgrader's version is greater than table's version.
      if (ProjectInfo.getVersion().compareTo(tableVersion) <= 0) {
        LOG.info("Table {} has already been upgraded. Its version is: {}", tableId, tableVersion);
        continue;
      }

      LOG.info("Starting upgrade for table {}", Bytes.toString(hTable.getTableName()));
      try {
        ScanBuilder scan = tableUtil.buildScan();
        scan.setTimeRange(0, HConstants.LATEST_TIMESTAMP);
        scan.addFamily(QueueEntryRow.COLUMN_FAMILY);
        scan.setMaxVersions(1); // we only need to see one version of each row
        List<Mutation> mutations = Lists.newArrayList();
        Result result;
        try (ResultScanner resultScanner = hTable.getScanner(scan.build())) {
          while ((result = resultScanner.next()) != null) {
            byte[] row = result.getRow();
            String rowKeyString = Bytes.toString(row);
            byte[] newKey = processRowKey(row);
            NavigableMap<byte[], byte[]> columnsMap = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
            if (newKey != null) {
              Put put = new Put(newKey);
              for (NavigableMap.Entry<byte[], byte[]> entry : columnsMap.entrySet()) {
                LOG.debug("Adding entry {} -> {} for upgrade",
                          Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
                put.add(QueueEntryRow.COLUMN_FAMILY, entry.getKey(), entry.getValue());
              }
              mutations.add(put);
              LOG.debug("Marking old key {} for deletion", rowKeyString);
              mutations.add(tableUtil.buildDelete(row).build());
            }
            LOG.info("Finished processing row key {}", rowKeyString);
          }
        }

        hTable.batch(mutations);

        LOG.info("Successfully completed upgrade for table {}", Bytes.toString(hTable.getTableName()));
      } catch (Exception e) {
        LOG.error("Error while upgrading table: {}", tableId, e);
        throw Throwables.propagate(e);
      } finally {
        hTable.close();
      }
    }
  }
}
