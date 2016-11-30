/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix.OneByteSimpleHash;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TableFactory} for creating messaging tables backed by HBase.
 */
public final class HBaseTableFactory implements TableFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableFactory.class);
  private static final byte[] COLUMN_FAMILY = {'d'};

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;
  private final ExecutorService scanExecutor;

  @Inject
  HBaseTableFactory(CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.tableUtil = tableUtil;

    RejectedExecutionHandler callerRunsPolicy = new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        LOG.info("No more threads in the HBase scan thread pool. Consider increase {}. Runnable from caller thread {}",
                 Constants.MessagingSystem.HBASE_MAX_SCAN_THREADS, Thread.currentThread().getName());
        // Runs it from the caller thread
        if (!executor.isShutdown()) {
          r.run();
        }
      }
    };
    // Creates a executor that will shrink to 0 threads if left idle
    // Uses daemon thread, hence no need to worry about shutdown
    // When all threads are busy, use the caller thread to execute
    this.scanExecutor = new ThreadPoolExecutor(0, cConf.getInt(Constants.MessagingSystem.HBASE_MAX_SCAN_THREADS),
                                               60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                                               Threads.createDaemonThreadFactory("messaging-hbase-scanner-%d"),
                                               callerRunsPolicy);
  }

  @Override
  public MetadataTable createMetadataTable(NamespaceId namespace, String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(namespace, tableName);
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      HTableDescriptor htd = tableUtil
        .buildHTableDescriptor(tableId)
        // Only stores the latest version
        .addFamily(new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(1))
        .build();
      tableUtil.createTableIfNotExists(admin, tableId, htd);
    }
    return new HBaseMetadataTable(tableUtil, tableUtil.createHTable(hConf, tableId), COLUMN_FAMILY);
  }

  @Override
  public MessageTable createMessageTable(NamespaceId namespace, String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(namespace, tableName);
    HTable hTable = createTableIfNotExists(tableId, cConf.getInt(Constants.MessagingSystem.MESSAGE_TABLE_HBASE_SPLITS));
    return new HBaseMessageTable(
      tableUtil, hTable, COLUMN_FAMILY,
      new RowKeyDistributorByHashPrefix(new OneByteSimpleHash(getKeyDistributorBuckets(hTable, tableId))),
      scanExecutor
    );
  }

  @Override
  public PayloadTable createPayloadTable(NamespaceId namespace, String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(namespace, tableName);
    HTable hTable = createTableIfNotExists(tableId, cConf.getInt(Constants.MessagingSystem.PAYLOAD_TABLE_HBASE_SPLITS));
    return new HBasePayloadTable(
      tableUtil, hTable, COLUMN_FAMILY,
      new RowKeyDistributorByHashPrefix(new OneByteSimpleHash(getKeyDistributorBuckets(hTable, tableId))),
      scanExecutor
    );
  }

  /**
   * Creates a new instance of {@link HTable} for the given {@link TableId}. If the hbase table doesn't exist,
   * a new one will be created with the given number of splits.
   */
  private HTable createTableIfNotExists(TableId tableId, int splits) throws IOException {
    // Create the table if the table doesn't exist
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      // Set the key distributor size the same as the initial number of splits, essentially one bucket per split.
      AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(new OneByteSimpleHash(splits));

      HTableDescriptor htd = tableUtil
        .buildHTableDescriptor(tableId)
        // Only stores the latest version
        .addFamily(new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(1))
        .setValue(Constants.MessagingSystem.KEY_DISTRIBUTOR_BUCKETS_ATTR, Integer.toString(splits))
        .build();

      byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits, splits, keyDistributor);
      tableUtil.createTableIfNotExists(admin, tableId, htd, splitKeys);
    }

    HTable hTable = tableUtil.createHTable(hConf, tableId);
    hTable.setAutoFlushTo(false);
    return hTable;
  }

  /**
   * Returns the value of the key distributor buckets attributed stored in the given HTable.
   */
  private int getKeyDistributorBuckets(HTable hTable, TableId tableId) throws IOException {
    // Get the actual key distributor buckets from the table. This can be different than the one in cConf
    // if the table was created before changes in the cConf.
    String bucketAttr = Constants.MessagingSystem.KEY_DISTRIBUTOR_BUCKETS_ATTR;
    try {
      String value = hTable.getTableDescriptor().getValue(bucketAttr);
      if (value == null) {
        // Cannot be null
        throw new IOException("Missing table attribute " + bucketAttr + " on HBase table " + tableId);
      }
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid value for table attribute " + bucketAttr + " on HBase table " + tableId, e);
    }
  }
}
