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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.AbstractQueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * admin for queues in hbase.
 */
@Singleton
public class HBaseQueueAdmin extends AbstractQueueAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueAdmin.class);

  /**
   * HBase table property for the number of bytes as the prefix of the queue entry row key.
   */
  public static final String PROPERTY_PREFIX_BYTES = "cdap.prefix.bytes";

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
                         TransactionExecutorFactory txExecutorFactory) {
    this(hConf, cConf, locationFactory, tableUtil, datasetFramework,
         txExecutorFactory, QueueConstants.QueueType.SHARDED_QUEUE);
  }

  protected HBaseQueueAdmin(Configuration hConf,
                            CConfiguration cConf,
                            LocationFactory locationFactory,
                            HBaseTableUtil tableUtil,
                            DatasetFramework datasetFramework,
                            TransactionExecutorFactory txExecutorFactory,
                            QueueConstants.QueueType type) {
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
    return TableId.from(namespace, QueueConstants.STATE_STORE_NAME + "."
                                  + HBaseQueueDatasetModule.STATE_STORE_EMBEDDED_TABLE_NAME);
  }

  protected final synchronized HBaseAdmin getHBaseAdmin() throws IOException {
    if (admin == null) {
      admin = new HBaseAdmin(hConf);
    }
    return admin;
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
    createStateStoreDataset(queueName.getFirstComponent());

    TableId tableId = getDataTableId(queueName);
    try (DatasetAdmin dsAdmin = new DatasetAdmin(tableId, hConf, tableUtil, properties)) {
      dsAdmin.create();
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
  public void clearAllForFlow(Id.Flow flowId) throws Exception {
    // all queues for a flow are in one table
    truncate(getDataTableId(flowId));
    // we also have to delete the config for these queues
    deleteFlowConfigs(flowId);
  }

  @Override
  public QueueConfigurer getQueueConfigurer(QueueName queueName) throws Exception {
    if (!exists(queueName)) {
      create(queueName);
    }
    return getConsumerStateStore(queueName);
  }

  @Override
  public void dropAllForFlow(Id.Flow flowId) throws Exception {
    // all queues for a flow are in one table
    drop(getDataTableId(flowId));
    // we also have to delete the config for these queues
    deleteFlowConfigs(flowId);
  }

  @Override
  public void upgrade() throws Exception {
    // For each table managed by this admin, perform an upgrade
    List<TableId> tableIds = tableUtil.listTables(getHBaseAdmin());
    List<TableId> stateStoreTableIds = Lists.newArrayList();

    for (TableId tableId : tableIds) {
      // It's important to skip config table enabled.
      if (isDataTable(tableId)) {
        LOG.info("Upgrading queue table: {}", tableId);
        Properties properties = new Properties();
        HTableDescriptor desc = tableUtil.getHTableDescriptor(getHBaseAdmin(), tableId);
        if (desc.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES) == null) {
          // It's the old queue table. Set the property prefix bytes to SALT_BYTES
          properties.setProperty(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES,
                                 Integer.toString(SaltedHBaseQueueStrategy.SALT_BYTES));
        }
        upgrade(tableId, properties);
        LOG.info("Upgraded queue table: {}", tableId);
      } else if (isStateStoreTable(tableId)) {
        stateStoreTableIds.add(tableId);
      }
    }

    // Upgrade of state store table
    for (TableId tableId : stateStoreTableIds) {
      LOG.info("Upgrading queue state store: {}", tableId);
      Id.DatasetInstance stateStoreId = createStateStoreDataset(tableId.getNamespace().getId());
      co.cask.cdap.api.dataset.DatasetAdmin admin = datasetFramework.getAdmin(stateStoreId, null);
      if (admin == null) {
        LOG.error("No dataset admin available for {}", stateStoreId);
        continue;
      }
      admin.upgrade();
      LOG.info("Upgraded queue state store: {}", tableId);
    }
  }

  QueueConstants.QueueType getType() {
    return type;
  }

  public HBaseConsumerStateStore getConsumerStateStore(QueueName queueName) throws Exception {
    Id.DatasetInstance stateStoreId = getStateStoreId(queueName.getFirstComponent());
    Map<String, String> args = ImmutableMap.of(HBaseQueueDatasetModule.PROPERTY_QUEUE_NAME, queueName.toString());
    HBaseConsumerStateStore stateStore = datasetFramework.getDataset(stateStoreId, args, null);
    if (stateStore == null) {
      throw new IllegalStateException("Consumer state store not exists for " + queueName);
    }
    return stateStore;
  }

  private Id.DatasetInstance getStateStoreId(String namespaceId) {
    return Id.DatasetInstance.from(namespaceId, QueueConstants.STATE_STORE_NAME);
  }

  private Id.DatasetInstance createStateStoreDataset(String namespace) throws IOException {
    try {
      Id.DatasetInstance stateStoreId = getStateStoreId(namespace);
      DatasetProperties configProperties = DatasetProperties.builder()
        .add(Table.PROPERTY_COLUMN_FAMILY, Bytes.toString(QueueEntryRow.COLUMN_FAMILY))
        .build();
      DatasetsUtil.createIfNotExists(datasetFramework, stateStoreId,
                                     HBaseQueueDatasetModule.STATE_STORE_TYPE_NAME, configProperties);
      return stateStoreId;
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    }
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
    try (final HBaseConsumerStateStore stateStore = getConsumerStateStore(queueName)) {
      Transactions.createTransactionExecutor(txExecutorFactory, stateStore)
        .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.clear();
        }
      });
    }
  }

  private void deleteFlowConfigs(Id.Flow flowId) throws Exception {
    // It's a bit hacky here since we know how the HBaseConsumerStateStore works.
    // Maybe we need another Dataset set that works across all queues.
    final QueueName prefixName = QueueName.from(URI.create(
      QueueName.prefixForFlow(flowId)));

    Id.DatasetInstance stateStoreId = getStateStoreId(flowId.getNamespaceId());
    Map<String, String> args = ImmutableMap.of(HBaseQueueDatasetModule.PROPERTY_QUEUE_NAME, prefixName.toString());
    HBaseConsumerStateStore stateStore = datasetFramework.getDataset(stateStoreId, args, null);
    if (stateStore == null) {
      // If the state store doesn't exists, meaning there is no queue, hence nothing to do.
      return;
    }

    try {
      final Table table = stateStore.getInternalTable();
      Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) table)
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            // Prefix name is "/" terminated ("queue:///namespace/app/flow/"), hence the scan is unique for the flow
            byte[] startRow = Bytes.toBytes(prefixName.toString());
            Scanner scanner = table.scan(startRow, Bytes.stopKeyForPrefix(startRow));
            try {
              Row row = scanner.next();
              while (row != null) {
                table.delete(row.getRow());
                row = scanner.next();
              }
            } finally {
              scanner.close();
            }
          }
        });
    } finally {
      stateStore.close();
    }
  }

  /**
   * @return coprocessors to set for the {@link HTable}
   */
  protected List<? extends Class<? extends Coprocessor>> getCoprocessors() {
    return ImmutableList.of(tableUtil.getQueueRegionObserverClassForVersion(),
                            tableUtil.getDequeueScanObserverClassForVersion());
  }

  @Override
  public void dropAllInNamespace(Id.Namespace namespaceId) throws Exception {
    Set<QueueConstants.QueueType> queueTypes = EnumSet.of(QueueConstants.QueueType.QUEUE,
                                                          QueueConstants.QueueType.SHARDED_QUEUE);
    for (QueueConstants.QueueType queueType : queueTypes) {
      // Note: The trailing "." is crucial, since otherwise nsId could match nsId1, nsIdx etc
      // It's important to keep config table enabled while disabling and dropping  queue tables.
      final String queueTableNamePrefix = String.format("%s.%s.", Id.Namespace.SYSTEM.getId(), queueType);
      final TableId configTableId = getConfigTableId(namespaceId.getId());
      tableUtil.deleteAllInNamespace(getHBaseAdmin(), namespaceId, new Predicate<TableId>() {
        @Override
        public boolean apply(TableId tableId) {
          // It's a bit hacky here since we know how the Dataset system names table
          return (tableId.getTableName().startsWith(queueTableNamePrefix)) && !tableId.equals(configTableId);
        }
      });
    }

    // Delete the state store in the namespace
    Id.DatasetInstance id = getStateStoreId(namespaceId.getId());
    if (datasetFramework.hasInstance(id)) {
      datasetFramework.deleteInstance(id);
    }
  }

  @Override
  public TableId getDataTableId(Id.Flow flowId) {
    return getDataTableId(flowId, type);
  }

  @Override
  public TableId getDataTableId(QueueName queueName) {
    return getDataTableId(queueName, type);
  }

  public TableId getDataTableId(QueueName queueName, QueueConstants.QueueType queueType) {
    if (!queueName.isQueue()) {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a queue.");
    }
    return getDataTableId(Id.Flow.from(queueName.getFirstComponent(),
                                       queueName.getSecondComponent(),
                                       queueName.getThirdComponent()),
                          queueType);
  }

  public TableId getDataTableId(Id.Flow flowId, QueueConstants.QueueType queueType) {
    String tableName = String.format("%s.%s.%s.%s", Id.Namespace.SYSTEM.getId(), queueType, flowId.getApplicationId(),
                                     flowId.getId());
    return TableId.from(flowId.getNamespaceId(), tableName);
  }

  private void upgrade(TableId tableId, Properties properties) throws Exception {
    try (AbstractHBaseDataSetAdmin dsAdmin = new DatasetAdmin(tableId, hConf, tableUtil, properties)) {
      dsAdmin.upgrade();
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
    if (tableName.split("\\.").length <= 3) {
      return false;
    }

    Set<QueueConstants.QueueType> queueTypes = EnumSet.of(QueueConstants.QueueType.QUEUE,
                                                          QueueConstants.QueueType.SHARDED_QUEUE);
    for (QueueConstants.QueueType queueType : queueTypes) {
      String prefix = Id.Namespace.SYSTEM.getId() + "." + queueType.toString();
      if (tableName.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private boolean isStateStoreTable(TableId tableId) {
    // Namespace doesn't matter
    return tableId.getTableName().equals(getConfigTableId("ns").getTableName());
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
      HTableDescriptorBuilder htd = tableUtil.buildHTableDescriptor(tableId);
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

      // Create queue table with splits. The distributor bucket size is the same as splits.
      int splits = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS);
      AbstractRowKeyDistributor distributor = new RowKeyDistributorByHashPrefix(
        new RowKeyDistributorByHashPrefix.OneByteSimpleHash(splits));

      byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits, splits, distributor);
      htd.setValue(QueueConstants.DISTRIBUTOR_BUCKETS, Integer.toString(splits));
      createQueueTable(tableId, htd, splitKeys);
    }

    private void createQueueTable(TableId tableId, HTableDescriptorBuilder htd, byte[][] splitKeys) throws IOException {
      int prefixBytes = (type == QueueConstants.QueueType.SHARDED_QUEUE) ? ShardedHBaseQueueStrategy.PREFIX_BYTES
                                                                         : SaltedHBaseQueueStrategy.SALT_BYTES;
      htd.setValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES, Integer.toString(prefixBytes));
      LOG.info("Create queue table with prefix bytes {}", htd.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES));
      tableUtil.createTableIfNotExists(getHBaseAdmin(), tableId, htd.build(), splitKeys);
    }
  }
}
