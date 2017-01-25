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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix.OneByteSimpleHash;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

/**
 * A {@link TableFactory} for creating messaging tables backed by HBase.
 * This factory memorize what HBase tables are already exists. If the table get deleted externally,
 * it relies on the {@link HBaseExceptionHandler} to cleanup the in memory cache so that on the next
 * request, the table will be recreated and the cache will get repopulated.
 *
 * Caching the the table descriptor is ok since all changes of the table descriptor should be done via
 * the TMS.
 */
public final class HBaseTableFactory implements TableFactory {

  public static final byte[] COLUMN_FAMILY = MessagingUtils.Constants.COLUMN_FAMILY;
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableFactory.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;
  private final ExecutorService scanExecutor;
  private final LocationFactory locationFactory;
  private final Map<TableId, HTableDescriptor> tableDescriptors;

  @Inject
  HBaseTableFactory(CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil,
                    LocationFactory locationFactory) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.tableUtil = tableUtil;
    this.locationFactory = locationFactory;
    this.tableDescriptors = new ConcurrentHashMap<>();

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
  public MetadataTable createMetadataTable(String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, tableName);
    HTable hTable = null;

    // If the table descriptor is in the cache, we assume the table exists.
    if (!tableDescriptors.containsKey(tableId)) {
      synchronized (this) {
        if (!tableDescriptors.containsKey(tableId)) {
          try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
            HTableDescriptor htd = tableUtil
              .buildHTableDescriptor(tableId)
              // Only stores the latest version
              .addFamily(new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(1))
              .build();
            tableUtil.createTableIfNotExists(admin, tableId, htd);
            hTable = tableUtil.createHTable(hConf, tableId);
            tableDescriptors.put(tableId, hTable.getTableDescriptor());
          }
        }
      }
    }
    if (hTable == null) {
      hTable = tableUtil.createHTable(hConf, tableId);
    }
    return new HBaseMetadataTable(tableUtil, hTable, COLUMN_FAMILY,
                                  cConf.getInt(Constants.MessagingSystem.HBASE_SCAN_CACHE_ROWS),
                                  createExceptionHandler(tableId));
  }

  @Override
  public MessageTable createMessageTable(String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, tableName);
    Class<? extends Coprocessor> tableCoprocessor = tableUtil.getMessageTableRegionObserverClassForVersion();
    HTableWithRowKeyDistributor tableWithRowKeyDistributor = createTable(
      tableId, cConf.getInt(Constants.MessagingSystem.MESSAGE_TABLE_HBASE_SPLITS), tableCoprocessor
    );
    return new HBaseMessageTable(
      tableUtil, tableWithRowKeyDistributor.getHTable(), COLUMN_FAMILY,
      tableWithRowKeyDistributor.getRowKeyDistributor(),
      scanExecutor, cConf.getInt(Constants.MessagingSystem.HBASE_SCAN_CACHE_ROWS),
      createExceptionHandler(tableId)
    );
  }

  @Override
  public PayloadTable createPayloadTable(String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, tableName);
    Class<? extends Coprocessor> tableCoprocessor = tableUtil.getPayloadTableRegionObserverClassForVersion();
    HTableWithRowKeyDistributor tableWithRowKeyDistributor = createTable(
      tableId, cConf.getInt(Constants.MessagingSystem.PAYLOAD_TABLE_HBASE_SPLITS), tableCoprocessor
    );
    return new HBasePayloadTable(
      tableUtil, tableWithRowKeyDistributor.getHTable(), COLUMN_FAMILY,
      tableWithRowKeyDistributor.getRowKeyDistributor(),
      scanExecutor, cConf.getInt(Constants.MessagingSystem.HBASE_SCAN_CACHE_ROWS),
      createExceptionHandler(tableId)
    );
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
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      disableTable(admin, tableId);
    }
  }

  public void disablePayloadTable(String tableName) throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, tableName);
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      disableTable(admin, tableId);
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
      public <T extends Exception> RuntimeException handleAndWrap(T exception) throws RuntimeException {
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

  private void disableTable(HBaseAdmin admin, TableId tableId) throws IOException {
    try {
      tableUtil.disableTable(admin, tableId);
      LOG.debug("TMS Table {} has been disabled", tableId);
    } catch (TableNotFoundException ex) {
      LOG.debug("TMS Table {} was not found. Skipping disable.", tableId, ex);
    } catch (TableNotEnabledException ex) {
      LOG.debug("TMS Table {} was already in disabled state.", tableId, ex);
    }
  }

  private void upgradeCoProcessor(TableId tableId, Class<? extends Coprocessor> coprocessor) throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      // If table doesn't exist, then skip upgrading coprocessor
      if (!tableUtil.tableExists(admin, tableId)) {
        LOG.debug("TMS Table {} was not found. Skip upgrading coprocessor.", tableId);
        return;
      }

      HTableDescriptor tableDescriptor = tableUtil.getHTableDescriptor(admin, tableId);

      // Get cdap version from the table
      ProjectInfo.Version version = HBaseTableUtil.getVersion(tableDescriptor);

      if (version.compareTo(ProjectInfo.getVersion()) >= 0) {
        // If cdap has version has not changed or is greater, no need to update
        LOG.info("Table '{}' has not changed and its version '{}' is same or greater than current CDAP version '{}'",
                 tableId, version, ProjectInfo.getVersion());
        return;
      }

      // create a new descriptor for the table update
      HTableDescriptorBuilder newDescriptor = tableUtil.buildHTableDescriptor(tableDescriptor);

      // Remove old coprocessor
      Map<String, HBaseTableUtil.CoprocessorInfo> coprocessorInfo = HBaseTableUtil.getCoprocessorInfo(tableDescriptor);
      for (Map.Entry<String, HBaseTableUtil.CoprocessorInfo> coprocessorEntry : coprocessorInfo.entrySet()) {
        newDescriptor.removeCoprocessor(coprocessorEntry.getValue().getClassName());
      }

      // Add new coprocessor
      addCoprocessor(coprocessor, newDescriptor);

      // Update CDAP version, table prefix
      HBaseTableUtil.setVersion(newDescriptor);
      HBaseTableUtil.setTablePrefix(newDescriptor, cConf);

      // Disable Table
      disableTable(admin, tableId);

      tableUtil.modifyTable(admin, newDescriptor.build());
      LOG.debug("Enabling table '{}'...", tableId);
      tableUtil.enableTable(admin, tableId);
    }

    LOG.info("Table '{}' update completed.", tableId);
  }

  /**
   * Creates a new instance of {@link HTable} for the given {@link TableId}. If the hbase table doesn't
   * exist, a new one will be created with the given number of splits.
   */
  private HTableWithRowKeyDistributor createTable(TableId tableId, int splits,
                                                  Class<? extends Coprocessor> coprocessor) throws IOException {

    // Lookup the table descriptor from the cache first. If it is there, we assume the HBase table exists
    // Otherwise, attempt to create it.
    HTable hTable = null;
    HTableDescriptor htd = tableDescriptors.get(tableId);

    if (htd == null) {
      synchronized (this) {
        htd = tableDescriptors.get(tableId);
        if (htd == null) {
          // Create the table if the table doesn't exist
          try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
            // If table exists, then skip creating coprocessor etc
            if (!tableUtil.tableExists(admin, tableId)) {
              TableId metadataTableId = tableUtil.createHTableId(NamespaceId.SYSTEM, cConf.get(
                Constants.MessagingSystem.METADATA_TABLE_NAME));

              HTableDescriptorBuilder htdBuilder = tableUtil
                .buildHTableDescriptor(tableId)
                // Only stores the latest version
                .addFamily(new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(1))
                // since we use 'OneByteSimpleHash', we set the number of prefix bytes attribute to 1
                .setValue(Constants.MessagingSystem.HBASE_MESSAGING_TABLE_PREFIX_NUM_BYTES, Integer.toString(1))
                .setValue(Constants.MessagingSystem.KEY_DISTRIBUTOR_BUCKETS_ATTR, Integer.toString(splits))
                .setValue(Constants.MessagingSystem.HBASE_METADATA_TABLE_NAMESPACE, metadataTableId.getNamespace());

              addCoprocessor(coprocessor, htdBuilder);
              htd = htdBuilder.build();

              // Set the key distributor size the same as the initial number of splits,
              // essentially one bucket per split.
              byte[][] splitKeys = HBaseTableUtil.getSplitKeys(
                splits, splits, new RowKeyDistributorByHashPrefix(new OneByteSimpleHash(splits)));
              tableUtil.createTableIfNotExists(admin, tableId, htd, splitKeys);

              hTable = tableUtil.createHTable(hConf, tableId);
              htd = hTable.getTableDescriptor();
              tableDescriptors.put(tableId, htd);
            } else {
              hTable = tableUtil.createHTable(hConf, tableId);
              htd = hTable.getTableDescriptor();
              tableDescriptors.put(tableId, htd);
            }
          }
        }
      }
    }

    if (hTable == null) {
      hTable = tableUtil.createHTable(hConf, tableId);
    }
    hTable.setAutoFlushTo(false);
    return new HTableWithRowKeyDistributor(
      hTable, new RowKeyDistributorByHashPrefix(new OneByteSimpleHash(getKeyDistributorBuckets(tableId, htd)))
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
        throw new IOException("Missing table attribute " + bucketAttr + " on HBase table " + tableId);
      }
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid value for table attribute " + bucketAttr + " on HBase table " + tableId, e);
    }
  }

  private void addCoprocessor(Class<? extends Coprocessor> coprocessor,
                              HTableDescriptorBuilder tableDescriptor) throws IOException {
    List<? extends Class<? extends Coprocessor>> coprocessors = ImmutableList.of(coprocessor);
    Location jarDir = locationFactory.create(cConf.get(Constants.MessagingSystem.COPROCESSOR_DIR));
    Location jarFile = HBaseTableUtil.createCoProcessorJar(coprocessor.getSimpleName(), jarDir, coprocessors);
    if (jarFile == null) {
      throw new IOException("Failed to create coprocessor jar for " + coprocessor);
    }
    tableDescriptor.addCoprocessor(coprocessor.getName(), new Path(jarFile.toURI().getPath()),
                                   Coprocessor.PRIORITY_USER, null);
  }

  /**
   * A holder class for {@link HTable} and a {@link AbstractRowKeyDistributor} for operating on the given table.
   */
  private static final class HTableWithRowKeyDistributor {
    private final HTable hTable;
    private final AbstractRowKeyDistributor rowKeyDistributor;

    private HTableWithRowKeyDistributor(HTable hTable, AbstractRowKeyDistributor rowKeyDistributor) {
      this.hTable = hTable;
      this.rowKeyDistributor = rowKeyDistributor;
    }

    HTable getHTable() {
      return hTable;
    }

    AbstractRowKeyDistributor getRowKeyDistributor() {
      return rowKeyDistributor;
    }
  }
}
