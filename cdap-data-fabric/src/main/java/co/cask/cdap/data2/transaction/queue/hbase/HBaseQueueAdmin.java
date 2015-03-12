/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.AbstractQueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static co.cask.cdap.data2.transaction.queue.QueueConstants.QueueType.QUEUE;

/**
 * admin for queues in hbase.
 */
@Singleton
public class HBaseQueueAdmin extends AbstractQueueAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueAdmin.class);

  // State store table name is system.queue.
  // The embedded table used in HBaseConsumerStateStore has the name "config", hence it will
  // map to "system.queue.config" for backward compatibility
  private static final String STATE_STORE_NAME
    = Constants.SYSTEM_NAMESPACE + "." + QueueConstants.QueueType.QUEUE.toString();

  /**
   * HBase table property for the number of bytes as the prefix of the queue entry row key.
   */
  public static final String PROPERTY_PREFIX_BYTES = "cdap.prefix.bytes";

  public static final int SALT_BYTES = 1;
  public static final int ROW_KEY_DISTRIBUTION_BUCKETS = 8;
  public static final AbstractRowKeyDistributor ROW_KEY_DISTRIBUTOR =
    new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(ROW_KEY_DISTRIBUTION_BUCKETS));
  // system.queue.config'
  private static final String CONFIG_TABLE_NAME =
    Constants.SYSTEM_NAMESPACE + "." + QueueConstants.QUEUE_CONFIG_TABLE_NAME;

  protected final HBaseTableUtil tableUtil;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final QueueConstants.QueueType type;
  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetFramework datasetFramework;

  private HBaseAdmin admin;

  @Inject
  public HBaseQueueAdmin(Configuration hConf,
                         CConfiguration cConf,
                         LocationFactory locationFactory,
                         HBaseTableUtil tableUtil,
                         DatasetFramework datasetFramework,
                         TransactionExecutorFactory txExecutorFactory) throws IOException {
    this(hConf, cConf, locationFactory, tableUtil, datasetFramework, txExecutorFactory, QUEUE);
  }

  protected HBaseQueueAdmin(Configuration hConf,
                            CConfiguration cConf,
                            LocationFactory locationFactory,
                            HBaseTableUtil tableUtil,
                            DatasetFramework datasetFramework,
                            TransactionExecutorFactory txExecutorFactory,
                            QueueConstants.QueueType type) throws IOException {
    super(type);
    this.hConf = hConf;
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.tableUtil = tableUtil;
    this.txExecutorFactory = txExecutorFactory;
    this.datasetFramework = datasetFramework;
    this.type = type;
  }

  public static TableId getConfigTableId(QueueName queueName) {
    return getConfigTableId(queueName.getFirstComponent());
  }

  public static TableId getConfigTableId(String namespace) {
    return TableId.from(namespace, CONFIG_TABLE_NAME);
  }

  protected final synchronized HBaseAdmin getHBaseAdmin() throws IOException {
    if (admin == null) {
      admin = new HBaseAdmin(hConf);
    }
    return admin;
  }

  /**
   * This determines whether dropping a queue is supported (by dropping the queue's table).
   */
  public boolean doDropTable(@SuppressWarnings("unused") QueueName queueName) {
    // no-op because this would drop all tables for the flow
    // todo: introduce a method dropAllFor(flow) or similar
    return false;
  }

  /**
   * This determines whether truncating a queue is supported (by truncating the queue's table).
   */
  public boolean doTruncateTable(@SuppressWarnings("unused") QueueName queueName) {
    // yes, this will truncate all queues of the flow. But it rarely makes sense to clear a single queue.
    // todo: introduce a method truncateAllFor(flow) or similar, and set this to false
    return true;
  }

  @Override
  public boolean exists(QueueName queueName) throws Exception {
    return tableUtil.tableExists(getHBaseAdmin(), getDataTableId(queueName)) &&
      datasetFramework.hasInstance(getStateStoreId(queueName.getFirstComponent()));
  }

  @Override
  public void create(QueueName queueName) throws IOException {
    create(queueName, new Properties());
  }

  @Override
  public void create(QueueName queueName, Properties properties) throws IOException {
    // Queue Config needs to be on separate table, otherwise disabling the queue table would makes queue config
    // not accessible by the queue region coprocessor for doing eviction.

    // Create the config table first so that in case the queue table coprocessor runs, it can access the config table.
    try {
      DatasetProperties configProperties = DatasetProperties.builder()
        .add(Table.PROPERTY_COLUMN_FAMILY, Bytes.toString(QueueEntryRow.COLUMN_FAMILY))
        .build();
      DatasetsUtil.createIfNotExists(datasetFramework,
                                     getStateStoreId(queueName.getFirstComponent()),
                                     HBaseQueueDatasetModule.STATE_STORE_TYPE_NAME, configProperties);
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    }

    TableId tableId = getDataTableId(queueName);
    DatasetAdmin dsAdmin = new DatasetAdmin(tableId, hConf, tableUtil, properties);
    try {
      dsAdmin.create();
    } finally {
      dsAdmin.close();
    }
  }

  @Override
  public void truncate(QueueName queueName) throws Exception {
    // all queues for one flow are stored in same table, and we would clear all of them. this makes it optional.
    if (doTruncateTable(queueName)) {
      truncate(getDataTableId(queueName));
      // If delete of state failed, the residue states won't affect the new queue created with the same name in future,
      // since states are recording the startRow, which has transaction info inside.
      deleteConsumerStates(queueName);
    } else {
      LOG.warn("truncate({}) on HBase queue table has no effect.", queueName);
    }
  }

  @Override
  public void clearAllForFlow(String namespaceId, String app, String flow) throws Exception {
    // all queues for a flow are in one table
    truncate(getDataTableId(namespaceId, app, flow));
    // we also have to delete the config for these queues
    deleteConsumerConfigurations(namespaceId, app, flow);
  }

  @Override
  public QueueConfigurer getQueueConfigurer(QueueName queueName) throws Exception {
    if (!exists(queueName)) {
      create(queueName);
    }
    return getConsumerStateStore(queueName);
  }

  @Override
  public void dropAllForFlow(String namespaceId, String app, String flow) throws Exception {
    // all queues for a flow are in one table
    drop(getDataTableId(namespaceId, app, flow));
    // we also have to delete the config for these queues
    deleteConsumerConfigurations(namespaceId, app, flow);
  }

  @Override
  public void upgrade() throws Exception {
    // For each table managed by this admin, perform an upgrade
    List<TableId> tableIds = tableUtil.listTables(getHBaseAdmin());
    for (TableId tableId : tableIds) {
      // It's important to skip config table enabled.
      if (isDataTable(tableId)) {
        LOG.info(String.format("Upgrading queue table: %s", tableId));
        Properties properties = new Properties();
        HTableDescriptor desc = tableUtil.getHTableDescriptor(getHBaseAdmin(), tableId);
        if (desc.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES) == null) {
          // It's the old queue table. Set the property prefix bytes to SALT_BYTES
          properties.setProperty(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES, Integer.toString(HBaseQueueAdmin.SALT_BYTES));
        }
        upgrade(tableId, properties);
        LOG.info(String.format("Upgraded queue table: %s", tableId));
      }
    }
  }

  // Only used by HBaseStreamAdmin
  void drop(QueueName queueName) throws Exception {
    // all queues for one flow are stored in same table, and we would drop all of them. this makes it optional.
    if (doDropTable(queueName)) {
      drop(getDataTableId(queueName));
      // If delete of state failed, the residue states won't affect the new queue created with the same name in future,
      // since states are recording the startRow, which has transaction info inside.
      deleteConsumerStates(queueName);
    } else {
      LOG.warn("drop({}) on HBase queue table has no effect.", queueName);
    }
  }

  @VisibleForTesting
  HBaseConsumerStateStore getConsumerStateStore(QueueName queueName) throws Exception {
    Id.DatasetInstance stateStoreId = getStateStoreId(queueName.getFirstComponent());
    Map<String, String> args = ImmutableMap.of(HBaseQueueDatasetModule.PROPERTY_QUEUE_NAME, queueName.toString());
    return datasetFramework.getDataset(stateStoreId, args, null);
  }

  private Id.DatasetInstance getStateStoreId(String namespaceId) {
    return Id.DatasetInstance.from(namespaceId, STATE_STORE_NAME);
  }

  private void truncate(TableId tableId) throws IOException {
    if (tableUtil.tableExists(getHBaseAdmin(), tableId)) {
      tableUtil.truncateTable(getHBaseAdmin(), tableId);
    }
  }


  private void drop(TableId tableId) throws IOException {
    if (tableUtil.tableExists(getHBaseAdmin(), tableId)) {
      tableUtil.dropTable(getHBaseAdmin(), tableId);
    }
  }

  /**
   * Deletes all consumer states associated with the given queue.
   */
  private void deleteConsumerStates(QueueName queueName) throws Exception {
    Id.DatasetInstance id = getStateStoreId(queueName.getFirstComponent());
    if (!datasetFramework.hasInstance(id)) {
      return;
    }
    final HBaseConsumerStateStore stateStore = getConsumerStateStore(queueName);
    Transactions.createTransactionExecutor(txExecutorFactory, stateStore).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        stateStore.clear();
      }
    });
  }

  private void deleteConsumerConfigurations(String namespaceId, String app, String flow) throws Exception {
    // It's a bit hacky here since we know how the HBaseConsumerStateStore works.
    // Maybe we need another Dataset set that works across all queues.
    QueueName prefixName = QueueName.from(URI.create(QueueName.prefixForFlow(namespaceId, app, flow)));
    deleteConsumerStates(prefixName);
  }

  /**
   * @return coprocessors to set for the {@link HTable}
   */
  protected List<? extends Class<? extends Coprocessor>> getCoprocessors() {
    return ImmutableList.of(tableUtil.getQueueRegionObserverClassForVersion(),
                            tableUtil.getDequeueScanObserverClassForVersion());
  }

  @Override
  public void dropAllInNamespace(String namespaceId) throws Exception {
    // Note: The trailing "." is crucial, since otherwise nsId could match nsId1, nsIdx etc
    // It's important to keep config table enabled while disabling and dropping  queue tables.
    final String queueTableNamePrefix = String.format("%s.", unqualifiedTableNamePrefix);
    final TableId configTableId = getConfigTableId(namespaceId);
    tableUtil.deleteAllInNamespace(getHBaseAdmin(), Id.Namespace.from(namespaceId), new Predicate<TableId>() {
      @Override
      public boolean apply(TableId tableId) {
        // It's a bit hacky here since we know how the Dataset system names table
        return (tableId.getTableName().startsWith(queueTableNamePrefix)) && !tableId.equals(configTableId);
      }
    });

    // Delete the state store in the namespace
    Id.DatasetInstance id = getStateStoreId(namespaceId);
    if (datasetFramework.hasInstance(id)) {
      datasetFramework.deleteInstance(id);
    }
  }

  private void upgrade(TableId tableId, Properties properties) throws Exception {
    AbstractHBaseDataSetAdmin dsAdmin = new DatasetAdmin(tableId, hConf, tableUtil, properties);
    try {
      dsAdmin.upgrade();
    } finally {
      dsAdmin.close();
    }
  }

  /**
   * @param tableId TableId being checked
   * @return true if the given table is the actual table for the queue (opposed to the config table for the queue
   * or tables for things other than queues).
   */
  private boolean isDataTable(TableId tableId) {
    // checks if table is constructed by getDataTableId
    String tableName = tableId.getTableName();
    String[] parts = tableName.split("\\.");
    if (parts.length != 4) {
      return false;
    }
    if (!Constants.SYSTEM_NAMESPACE.equals(parts[0])) {
      return false;
    }
    return type.toString().equals(parts[1]);
  }

  // only used for create & upgrade of data table
  private final class DatasetAdmin extends AbstractHBaseDataSetAdmin {
    private final Properties properties;

    private DatasetAdmin(TableId tableId, Configuration hConf, HBaseTableUtil tableUtil, Properties properties) {
      super(tableId, hConf, tableUtil);
      this.properties = properties;
    }

    @Override
    protected CoprocessorJar createCoprocessorJar() throws IOException {
      List<? extends Class<? extends Coprocessor>> coprocessors = getCoprocessors();
      if (coprocessors.isEmpty()) {
        return CoprocessorJar.EMPTY;
      }

      Location jarDir = locationFactory.create(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                                                         QueueConstants.DEFAULT_QUEUE_TABLE_COPROCESSOR_DIR));
      Location jarFile = HBaseTableUtil.createCoProcessorJar(type.toString(), jarDir, coprocessors);
      return new CoprocessorJar(coprocessors, jarFile);
    }

    @Override
    protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
      boolean updated = false;
      HColumnDescriptor columnDescriptor = tableDescriptor.getFamily(QueueEntryRow.COLUMN_FAMILY);
      if (columnDescriptor.getMaxVersions() != 1) {
        columnDescriptor.setMaxVersions(1);
        updated = true;
      }
      for (String key : properties.stringPropertyNames()) {
        String oldValue = tableDescriptor.getValue(key);
        String newValue = properties.getProperty(key);
        if (!Objects.equal(oldValue, newValue)) {
          tableDescriptor.setValue(key, newValue);
          updated = true;
        }
      }
      return updated;
    }

    @Override
    public void create() throws IOException {
      // Create the queue table
      HTableDescriptor htd = tableUtil.createHTableDescriptor(tableId);
      for (String key : properties.stringPropertyNames()) {
        htd.setValue(key, properties.getProperty(key));
      }

      HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
      hcd.setMaxVersions(1);
      htd.addFamily(hcd);

      // Add coprocessors
      CoprocessorJar coprocessorJar = createCoprocessorJar();
      for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
        addCoprocessor(htd, coprocessor, coprocessorJar.getJarLocation(), coprocessorJar.getPriority(coprocessor));
      }

      // Create queue table with splits.
      int splits = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS,
                                QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS);
      byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits);
      createQueueTable(tableId, htd, splitKeys);
    }

    private void createQueueTable(TableId tableId, HTableDescriptor htd, byte[][] splitKeys) throws IOException {
      if (htd.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES) == null) {
        htd.setValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES, Integer.toString(HBaseQueueAdmin.SALT_BYTES));
      }
      LOG.info("Create queue table with prefix bytes {}", htd.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES));
      tableUtil.createTableIfNotExists(getHBaseAdmin(), tableId, htd, splitKeys);
    }
  }
}
