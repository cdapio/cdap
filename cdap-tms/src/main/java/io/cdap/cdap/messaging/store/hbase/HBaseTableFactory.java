/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.hbase;

import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.ColumnFamilyDescriptorBuilder;
import io.cdap.cdap.data2.util.hbase.CoprocessorManager;
import io.cdap.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseVersion;
import io.cdap.cdap.data2.util.hbase.HTableDescriptorBuilder;
import io.cdap.cdap.data2.util.hbase.HTableNameConverter;
import io.cdap.cdap.data2.util.hbase.TableDescriptorBuilder;
import io.cdap.cdap.hbase.wd.AbstractRowKeyDistributor;
import io.cdap.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import io.cdap.cdap.hbase.wd.RowKeyDistributorByHashPrefix.OneByteSimpleHash;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.hbase.CoprocessorDescriptor;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TableFactory} for creating messaging tables backed by HBase. This factory memorize what
 * HBase tables are already exists. If the table get deleted externally, it relies on the {@link
 * HBaseExceptionHandler} to cleanup the in memory cache so that on the next request, the table will
 * be recreated and the cache will get repopulated.
 *
 * Caching the the table descriptor is ok since all changes of the table descriptor should be done
 * via the TMS.
 */
public final class HBaseTableFactory implements TableFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableFactory.class);
  // Exponentially log less on executor rejected execution due to limit threads
  private static final Logger REJECTION_LOG = Loggers.sampling(LOG,
      LogSamplers.exponentialLimit(1, 1024, 2.0d));

  public static final byte[] COLUMN_FAMILY = MessagingUtils.Constants.COLUMN_FAMILY;

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;
  private final ExecutorService scanExecutor;
  private final Map<TableId, HTableDescriptor> tableDescriptors;
  private final CoprocessorManager coprocessorManager;
  private final HBaseDDLExecutorFactory ddlExecutorFactory;
  private final String metadataTableName;
  private final String messageTableName;
  private final String payloadTableName;

  @Inject
  HBaseTableFactory(CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil,
      LocationFactory locationFactory) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.tableUtil = tableUtil;
    this.tableDescriptors = new ConcurrentHashMap<>();
    this.coprocessorManager = new CoprocessorManager(cConf, locationFactory, tableUtil);
    // Currently we don't support customizable table name yet, hence always get it from cConf.
    // Later on it can be done by topic properties, with impersonation setting as well.
    this.metadataTableName = cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME);
    this.messageTableName = cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME);
    this.payloadTableName = cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME);

    RejectedExecutionHandler callerRunsPolicy = new RejectedExecutionHandler() {

      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        REJECTION_LOG.info(
            "No more threads in the HBase scan thread pool. Consider increase {}. Scan from caller thread {}",
            Constants.MessagingSystem.HBASE_MAX_SCAN_THREADS, Thread.currentThread().getName()
        );
        // Runs it from the caller thread
        if (!executor.isShutdown()) {
          r.run();
        }
      }
    };
    // Creates a executor that will shrink to 0 threads if left idle
    // Uses daemon thread, hence no need to worry about shutdown
    // When all threads are busy, use the caller thread to execute
    this.scanExecutor = new ThreadPoolExecutor(0,
        cConf.getInt(Constants.MessagingSystem.HBASE_MAX_SCAN_THREADS),
        60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        Threads.createDaemonThreadFactory("messaging-hbase-scanner-%d"),
        callerRunsPolicy);

    this.ddlExecutorFactory = new HBaseDDLExecutorFactory(cConf, hConf);
  }

  @Override
  public MetadataTable createMetadataTable() throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, metadataTableName);
    Table table = null;

    // If the table descriptor is in the cache, we assume the table exists.
    if (!tableDescriptors.containsKey(tableId)) {
      synchronized (this) {
        if (!tableDescriptors.containsKey(tableId)) {
          try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {

            ColumnFamilyDescriptorBuilder cfdBuilder =
                HBaseTableUtil.getColumnFamilyDescriptorBuilder(Bytes.toString(COLUMN_FAMILY),
                    hConf);

            TableDescriptorBuilder tdBuilder =
                HBaseTableUtil.getTableDescriptorBuilder(tableId, cConf)
                    .addColumnFamily(cfdBuilder.build());

            ddlExecutor.createTableIfNotExists(tdBuilder.build(), null);
            table = tableUtil.createTable(hConf, tableId);
            tableDescriptors.put(tableId, table.getTableDescriptor());
          }
        }
      }
    }
    if (table == null) {
      table = tableUtil.createTable(hConf, tableId);
    }
    return new HBaseMetadataTable(tableUtil, table, COLUMN_FAMILY,
        cConf.getInt(Constants.MessagingSystem.HBASE_SCAN_CACHE_ROWS),
        createExceptionHandler(tableId));
  }

  @Override
  public MessageTable createMessageTable(TopicMetadata topicMetadata) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, messageTableName);
    Class<? extends Coprocessor> tableCoprocessor = tableUtil.getMessageTableRegionObserverClassForVersion();
    HTableWithRowKeyDistributor tableWithRowKeyDistributor = createTable(
        tableId, cConf.getInt(Constants.MessagingSystem.MESSAGE_TABLE_HBASE_SPLITS),
        tableCoprocessor
    );
    return new HBaseMessageTable(
        tableUtil, tableWithRowKeyDistributor.getTable(), COLUMN_FAMILY,
        tableWithRowKeyDistributor.getRowKeyDistributor(),
        scanExecutor, cConf.getInt(Constants.MessagingSystem.HBASE_SCAN_CACHE_ROWS),
        createExceptionHandler(tableId)
    );
  }

  @Override
  public PayloadTable createPayloadTable(TopicMetadata topicMetadata) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, payloadTableName);
    Class<? extends Coprocessor> tableCoprocessor = tableUtil.getPayloadTableRegionObserverClassForVersion();
    HTableWithRowKeyDistributor tableWithRowKeyDistributor = createTable(
        tableId, cConf.getInt(Constants.MessagingSystem.PAYLOAD_TABLE_HBASE_SPLITS),
        tableCoprocessor
    );
    return new HBasePayloadTable(
        tableUtil, tableWithRowKeyDistributor.getTable(), COLUMN_FAMILY,
        tableWithRowKeyDistributor.getRowKeyDistributor(),
        scanExecutor, cConf.getInt(Constants.MessagingSystem.HBASE_SCAN_CACHE_ROWS),
        createExceptionHandler(tableId)
    );
  }

  @Override
  public void close() {
    // no-op
  }

  public void upgradeMessageTable(String tableName) throws IOException {
    upgradeCoProcessor(tableUtil.createHTableId(NamespaceId.SYSTEM, tableName),
        tableUtil.getMessageTableRegionObserverClassForVersion());
  }

  public void upgradePayloadTable(String tableName) throws IOException {
    upgradeCoProcessor(tableUtil.createHTableId(NamespaceId.SYSTEM, tableName),
        tableUtil.getPayloadTableRegionObserverClassForVersion());
  }

  public void disableMessageTable(String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, tableName);
    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      disableTable(ddlExecutor, tableId);
    }
  }

  public void disablePayloadTable(String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, tableName);
    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      disableTable(ddlExecutor, tableId);
    }
  }

  /**
   * Creates a {@link HBaseExceptionHandler} that will invalidate the cached table descriptor if
   * {@link TableNotFoundException} is encountered while performing hbase operation.
   */
  private HBaseExceptionHandler createExceptionHandler(final TableId tableId) {
    return new HBaseExceptionHandler() {
      @Override
      public <T extends Exception> T handle(@Nonnull T exception) throws T {
        if (exception instanceof TableNotFoundException) {
          tableDescriptors.remove(tableId);
        } else if (exception instanceof RetriesExhaustedWithDetailsException) {
          for (Throwable cause : ((RetriesExhaustedWithDetailsException) exception).getCauses()) {
            if (cause instanceof TableNotFoundException) {
              tableDescriptors.remove(tableId);
              break;
            }
          }
        }
        throw exception;
      }

      @Override
      public <T extends Exception> RuntimeException handleAndWrap(T exception)
          throws RuntimeException {
        try {
          throw handle(exception);
        } catch (Throwable t) {
          if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
          }
          throw new RuntimeException(t);
        }
      }
    };
  }

  private void enableTable(HBaseDDLExecutor ddlExecutor, TableId tableId) throws IOException {
    try {
      TableName tableName = HTableNameConverter.toTableName(
          cConf.get(Constants.Dataset.TABLE_PREFIX), tableId);
      ddlExecutor.enableTableIfDisabled(tableName.getNamespaceAsString(),
          tableName.getQualifierAsString());
      LOG.debug("TMS Table {} has been enabled.", tableName);
    } catch (TableNotFoundException ex) {
      LOG.debug("TMS Table {} was not found. Skipping enable.", tableId, ex);
    } catch (TableNotDisabledException ex) {
      LOG.debug("TMS Table {} was already in enabled state.", tableId, ex);
    }
  }

  private void disableTable(HBaseDDLExecutor ddlExecutor, TableId tableId) throws IOException {
    try {
      TableName tableName = HTableNameConverter.toTableName(
          cConf.get(Constants.Dataset.TABLE_PREFIX), tableId);
      ddlExecutor.disableTableIfEnabled(tableName.getNamespaceAsString(),
          tableName.getQualifierAsString());
      LOG.debug("TMS Table {} has been disabled.", tableId);
    } catch (TableNotFoundException ex) {
      LOG.debug("TMS Table {} was not found. Skipping disable.", tableId, ex);
    } catch (TableNotEnabledException ex) {
      LOG.debug("TMS Table {} was already in disabled state.", tableId, ex);
    }
  }

  private void upgradeCoProcessor(TableId tableId, Class<? extends Coprocessor> coprocessor)
      throws IOException {
    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      HTableDescriptor tableDescriptor;
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        // If table doesn't exist, then skip upgrading coprocessor
        if (!tableUtil.tableExists(admin, tableId)) {
          LOG.debug("TMS Table {} was not found. Skip upgrading coprocessor.", tableId);
          return;
        }

        tableDescriptor = tableUtil.getHTableDescriptor(admin, tableId);
      }

      // Get cdap version from the table
      ProjectInfo.Version version = HBaseTableUtil.getVersion(tableDescriptor);
      String hbaseVersion = HBaseTableUtil.getHBaseVersion(tableDescriptor);

      if (hbaseVersion != null
          && hbaseVersion.equals(HBaseVersion.getVersionString())
          && version.compareTo(ProjectInfo.getVersion()) >= 0) {
        // If cdap has version has not changed or is greater, no need to update. Just enable it, in case
        // it has been disabled by the upgrade tool, and return
        LOG.info(
            "Table '{}' has not changed and its version '{}' is same or greater than current CDAP version '{}'."

                + " The underlying HBase version {} has also not changed.",
            tableId, version, ProjectInfo.getVersion(), hbaseVersion);
        enableTable(ddlExecutor, tableId);
        return;
      }

      // create a new descriptor for the table update
      HTableDescriptorBuilder newDescriptor = tableUtil.buildHTableDescriptor(tableDescriptor);

      // Remove old coprocessor
      Map<String, HBaseTableUtil.CoprocessorInfo> coprocessorInfo = HBaseTableUtil.getCoprocessorInfo(
          tableDescriptor);
      for (Map.Entry<String, HBaseTableUtil.CoprocessorInfo> coprocessorEntry : coprocessorInfo.entrySet()) {
        newDescriptor.removeCoprocessor(coprocessorEntry.getValue().getClassName());
      }

      // Add new coprocessor
      CoprocessorDescriptor coprocessorDescriptor =
          coprocessorManager.getCoprocessorDescriptor(coprocessor, Coprocessor.PRIORITY_USER);
      Path path = coprocessorDescriptor.getPath() == null ? null
          : new Path(coprocessorDescriptor.getPath());
      newDescriptor.addCoprocessor(coprocessorDescriptor.getClassName(), path,
          coprocessorDescriptor.getPriority(),
          coprocessorDescriptor.getProperties());

      // Update CDAP version, table prefix
      HBaseTableUtil.setVersion(newDescriptor);
      HBaseTableUtil.setHBaseVersion(newDescriptor);
      HBaseTableUtil.setTablePrefix(newDescriptor, cConf);
      // Disable auto-splitting
      newDescriptor.setValue(HTableDescriptor.SPLIT_POLICY,
          cConf.get(Constants.MessagingSystem.TABLE_HBASE_SPLIT_POLICY));

      // Disable Table
      disableTable(ddlExecutor, tableId);

      tableUtil.modifyTable(ddlExecutor, newDescriptor.build());
      LOG.debug("Enabling table '{}'...", tableId);
      enableTable(ddlExecutor, tableId);
    }

    LOG.info("Table '{}' update completed.", tableId);
  }

  /**
   * Creates a new instance of {@link Table} for the given {@link TableId}. If the hbase table
   * doesn't exist, a new one will be created with the given number of splits.
   */
  private HTableWithRowKeyDistributor createTable(TableId tableId, int splits,
      Class<? extends Coprocessor> coprocessor) throws IOException {

    // Lookup the table descriptor from the cache first. If it is there, we assume the HBase table exists
    // Otherwise, attempt to create it.
    Table table = null;
    HTableDescriptor htd = tableDescriptors.get(tableId);

    if (htd == null) {
      synchronized (this) {
        htd = tableDescriptors.get(tableId);
        if (htd == null) {
          boolean tableExists;
          try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
            tableExists = tableUtil.tableExists(admin, tableId);
          }

          // Create the table if the table doesn't exist
          try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
            // If table exists, then skip creating coprocessor etc
            if (!tableExists) {
              TableId metadataTableId = tableUtil.createHTableId(
                  NamespaceId.SYSTEM, cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME));

              ColumnFamilyDescriptorBuilder cfdBuilder =
                  HBaseTableUtil.getColumnFamilyDescriptorBuilder(Bytes.toString(COLUMN_FAMILY),
                      hConf);

              TableDescriptorBuilder tdBuilder = HBaseTableUtil.getTableDescriptorBuilder(tableId,
                      cConf)
                  .addColumnFamily(cfdBuilder.build())
                  .addProperty(Constants.MessagingSystem.HBASE_MESSAGING_TABLE_PREFIX_NUM_BYTES,
                      Integer.toString(1))
                  .addProperty(Constants.MessagingSystem.KEY_DISTRIBUTOR_BUCKETS_ATTR,
                      Integer.toString(splits))
                  .addProperty(Constants.MessagingSystem.HBASE_METADATA_TABLE_NAMESPACE,
                      metadataTableId.getNamespace())
                  // Disable auto-splitting
                  .addProperty(HTableDescriptor.SPLIT_POLICY,
                      cConf.get(Constants.MessagingSystem.TABLE_HBASE_SPLIT_POLICY))
                  .addCoprocessor(coprocessorManager.getCoprocessorDescriptor(coprocessor,
                      Coprocessor.PRIORITY_USER));

              // Set the key distributor size the same as the initial number of splits,
              // essentially one bucket per split.
              byte[][] splitKeys = HBaseTableUtil.getSplitKeys(
                  splits, splits, new RowKeyDistributorByHashPrefix(new OneByteSimpleHash(splits)));
              ddlExecutor.createTableIfNotExists(tdBuilder.build(), splitKeys);

              table = tableUtil.createTable(hConf, tableId);
              htd = table.getTableDescriptor();
              tableDescriptors.put(tableId, htd);
            } else {
              table = tableUtil.createTable(hConf, tableId);
              htd = table.getTableDescriptor();
              tableDescriptors.put(tableId, htd);
            }
          }
        }
      }
    }

    if (table == null) {
      table = tableUtil.createTable(hConf, tableId);
    }
    return new HTableWithRowKeyDistributor(
        table, new RowKeyDistributorByHashPrefix(
        new OneByteSimpleHash(getKeyDistributorBuckets(tableId, htd)))
    );
  }

  /**
   * Returns the value of the key distributor buckets attributed stored in the given HTable.
   */
  private int getKeyDistributorBuckets(TableId tableId, HTableDescriptor htd) throws IOException {
    // Get the actual key distributor buckets from the table descriptor. This can be different than the one in cConf
    // if the table was created before changes in the cConf.
    String bucketAttr = Constants.MessagingSystem.KEY_DISTRIBUTOR_BUCKETS_ATTR;
    try {
      String value = htd.getValue(bucketAttr);
      if (value == null) {
        // Cannot be null
        throw new IOException(
            "Missing table attribute " + bucketAttr + " on HBase table " + tableId);
      }
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IOException(
          "Invalid value for table attribute " + bucketAttr + " on HBase table " + tableId, e);
    }
  }

  /**
   * A holder class for {@link Table} and a {@link AbstractRowKeyDistributor} for operating on the
   * given table.
   */
  private static final class HTableWithRowKeyDistributor {

    private final Table table;
    private final AbstractRowKeyDistributor rowKeyDistributor;

    private HTableWithRowKeyDistributor(Table table, AbstractRowKeyDistributor rowKeyDistributor) {
      this.table = table;
      this.rowKeyDistributor = rowKeyDistributor;
    }

    Table getTable() {
      return table;
    }

    AbstractRowKeyDistributor getRowKeyDistributor() {
      return rowKeyDistributor;
    }
  }
}
