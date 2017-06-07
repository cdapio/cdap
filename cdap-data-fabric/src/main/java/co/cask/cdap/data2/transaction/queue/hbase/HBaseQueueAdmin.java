/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.ProgramContext;
import co.cask.cdap.data.ProgramContextAware;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.AbstractQueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.ColumnFamilyDescriptorBuilder;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.TableDescriptorBuilder;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.ImpersonationUtils;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * admin for queues in hbase.
 */
@Singleton
public class HBaseQueueAdmin extends AbstractQueueAdmin implements ProgramContextAware {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueAdmin.class);

  /**
   * HBase table property for the number of bytes as the prefix of the queue entry row key.
   */
  public static final String PROPERTY_PREFIX_BYTES = "cdap.prefix.bytes";

  protected final HBaseTableUtil tableUtil;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final QueueConstants.QueueType type;
  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetFramework datasetFramework;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Impersonator impersonator;
  private final HBaseDDLExecutorFactory ddlExecutorFactory;
  private final LocationFactory locationFactory;

  @Inject
  HBaseQueueAdmin(Configuration hConf,
                  CConfiguration cConf,
                  LocationFactory locationFactory,
                  HBaseTableUtil tableUtil,
                  DatasetFramework datasetFramework,
                  TransactionExecutorFactory txExecutorFactory,
                  NamespaceQueryAdmin namespaceQueryAdmin,
                  Impersonator impersonator) {

    this(hConf, cConf, locationFactory, tableUtil, datasetFramework, txExecutorFactory,
         QueueConstants.QueueType.SHARDED_QUEUE, namespaceQueryAdmin, impersonator);
  }

  protected HBaseQueueAdmin(Configuration hConf,
                            CConfiguration cConf,
                            LocationFactory locationFactory,
                            HBaseTableUtil tableUtil,
                            DatasetFramework datasetFramework,
                            TransactionExecutorFactory txExecutorFactory,
                            QueueConstants.QueueType type,
                            NamespaceQueryAdmin namespaceQueryAdmin,
                            Impersonator impersonator) {
    super(type);
    this.hConf = hConf;
    this.cConf = cConf;
    this.tableUtil = tableUtil;
    this.txExecutorFactory = txExecutorFactory;
    this.datasetFramework = datasetFramework;
    this.type = type;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.impersonator = impersonator;
    this.locationFactory = locationFactory;
    this.ddlExecutorFactory = new HBaseDDLExecutorFactory(cConf, hConf);
  }

  @Override
  public void setContext(ProgramContext programContext) {
    if (datasetFramework instanceof ProgramContextAware) {
      ((ProgramContextAware) datasetFramework).setContext(programContext);
    }
  }

  public static String getConfigTableName() {
    return QueueConstants.STATE_STORE_NAME + "." + HBaseQueueDatasetModule.STATE_STORE_EMBEDDED_TABLE_NAME;
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
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      return tableUtil.tableExists(admin, getDataTableId(queueName)) &&
        datasetFramework.hasInstance(getStateStoreId(queueName.getFirstComponent()));
    }
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
    try (QueueDatasetAdmin dsAdmin = new QueueDatasetAdmin(tableId, hConf, cConf, tableUtil, properties)) {
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
  public void clearAllForFlow(FlowId flowId) throws Exception {
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
  public void dropAllForFlow(FlowId flowId) throws Exception {
    // all queues for a flow are in one table
    drop(getDataTableId(flowId));
    // we also have to delete the config for these queues
    deleteFlowConfigs(flowId);
  }

  @Override
  public void upgrade() throws Exception {
    int numThreads = cConf.getInt(Constants.Upgrade.UPGRADE_THREAD_POOL_SIZE);
    final ExecutorService executor =
      Executors.newFixedThreadPool(numThreads,
                                   new ThreadFactoryBuilder()
                                     .setNameFormat("hbase-cmd-executor-%d")
                                     .setDaemon(true)
                                     .build());

    final List<Closeable> toClose = new ArrayList<>();
    try {
      final Map<TableId, Future<?>> allFutures = new HashMap<>();
      // For each queue config table and queue data table in each namespace, perform an upgrade
      for (final NamespaceMeta namespaceMeta : namespaceQueryAdmin.list()) {
        impersonator.doAs(namespaceMeta.getNamespaceId(), new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(hConf);
            // register it for close, after all Futures are complete
            toClose.add(hBaseAdmin);
            Map<TableId, Future<?>> futures = upgradeQueues(namespaceMeta, executor, hBaseAdmin);
            allFutures.putAll(futures);
            return null;
          }
        });
      }

      // Wait for the queue upgrades to complete
      Map<TableId, Throwable> failed = waitForUpgrade(allFutures);
      if (!failed.isEmpty()) {
        for (Map.Entry<TableId, Throwable> entry : failed.entrySet()) {
          LOG.error("Failed to upgrade queue table {}", entry.getKey(), entry.getValue());
        }
        throw new Exception(String.format("Error upgrading queue tables. %s of %s failed",
                                          failed.size(), allFutures.size()));
      }
    } finally {
      for (Closeable closeable : toClose) {
        Closeables.closeQuietly(closeable);
      }
      // We'll have tasks pending in the executor only on an interrupt, when user wants to abort the upgrade.
      // Use shutdownNow() to interrupt the tasks and abort.
      executor.shutdownNow();
    }
  }

  private Map<TableId, Future<?>> upgradeQueues(final NamespaceMeta namespaceMeta, ExecutorService executor,
                                                final HBaseAdmin admin) throws Exception {
    String hbaseNamespace = tableUtil.getHBaseNamespace(namespaceMeta);
    List<TableId> tableIds = tableUtil.listTablesInNamespace(admin, hbaseNamespace);
    List<TableId> stateStoreTableIds = Lists.newArrayList();
    Map<TableId, Future<?>> futures = new HashMap<>();

    for (final TableId tableId : tableIds) {
      // It's important to skip config table enabled.
      if (isDataTable(tableId)) {
        Callable<Void> callable = new Callable<Void>() {
          public Void call() throws Exception {
            LOG.info("Upgrading queue table: {}", tableId);
            Properties properties = new Properties();
            HTableDescriptor desc = tableUtil.getHTableDescriptor(admin, tableId);
            if (desc.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES) == null) {
              // It's the old queue table. Set the property prefix bytes to SALT_BYTES
              properties.setProperty(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES,
                                     Integer.toString(SaltedHBaseQueueStrategy.SALT_BYTES));
            }
            upgrade(tableId, properties);
            LOG.info("Upgraded queue table: {}", tableId);
            return null;
          }
        };
        Future<?> future =
          executor.submit(ImpersonationUtils.createImpersonatingCallable(impersonator, namespaceMeta, callable));
        futures.put(tableId, future);
      } else if (isStateStoreTable(tableId)) {
        stateStoreTableIds.add(tableId);
      }
    }

    // Upgrade of state store table
    for (final TableId tableId : stateStoreTableIds) {
      Callable<Void> callable = new Callable<Void>() {
        public Void call() throws Exception {
          LOG.info("Upgrading queue state store: {}", tableId);
          DatasetId stateStoreId = createStateStoreDataset(namespaceMeta.getName());
          DatasetAdmin datasetAdmin = datasetFramework.getAdmin(stateStoreId, null);
          if (datasetAdmin == null) {
            LOG.error("No dataset admin available for {}", stateStoreId);
            return null;
          }
          datasetAdmin.upgrade();
          LOG.info("Upgraded queue state store: {}", tableId);
          return null;
        }
      };
      Future<?> future =
        executor.submit(ImpersonationUtils.createImpersonatingCallable(impersonator, namespaceMeta, callable));
      futures.put(tableId, future);
    }

    return futures;
  }

  QueueConstants.QueueType getType() {
    return type;
  }

  public HBaseConsumerStateStore getConsumerStateStore(QueueName queueName) throws Exception {
    DatasetId stateStoreId = getStateStoreId(queueName.getFirstComponent());
    Map<String, String> args = ImmutableMap.of(HBaseQueueDatasetModule.PROPERTY_QUEUE_NAME, queueName.toString());
    HBaseConsumerStateStore stateStore = datasetFramework.getDataset(stateStoreId, args, null);
    if (stateStore == null) {
      throw new IllegalStateException("Consumer state store not exists for " + queueName);
    }
    return stateStore;
  }

  private DatasetId getStateStoreId(String namespaceId) {
    return new DatasetId(namespaceId, QueueConstants.STATE_STORE_NAME);
  }

  private DatasetId createStateStoreDataset(String namespace) throws IOException {
    try {
      DatasetId stateStoreId = getStateStoreId(namespace);
      DatasetProperties configProperties =
        TableProperties.builder().setColumnFamily(QueueEntryRow.COLUMN_FAMILY).build();
      DatasetsUtil.createIfNotExists(datasetFramework, stateStoreId,
                                     HBaseQueueDatasetModule.STATE_STORE_TYPE_NAME, configProperties);
      return stateStoreId;
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    }
  }

  private void truncate(TableId tableId) throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      if (!tableUtil.tableExists(admin, tableId)) {
        return;
      }
    }

    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      tableUtil.truncateTable(ddlExecutor, tableId);
    }
  }

  private void drop(TableId tableId) throws IOException {
    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get();
         HBaseAdmin admin = new HBaseAdmin(hConf)) {
      if (tableUtil.tableExists(admin, tableId)) {
        tableUtil.dropTable(ddlExecutor, tableId);
      }
    }
  }

  /**
   * Deletes all consumer states associated with the given queue.
   */
  private void deleteConsumerStates(QueueName queueName) throws Exception {
    DatasetId id = getStateStoreId(queueName.getFirstComponent());
    if (!datasetFramework.hasInstance(id)) {
      return;
    }
    try (HBaseConsumerStateStore stateStore = getConsumerStateStore(queueName)) {
      Transactions.createTransactionExecutor(txExecutorFactory, stateStore)
        .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.clear();
        }
      });
    }
  }

  private void deleteFlowConfigs(FlowId flowId) throws Exception {
    // It's a bit hacky here since we know how the HBaseConsumerStateStore works.
    // Maybe we need another Dataset set that works across all queues.
    final QueueName prefixName = QueueName.from(URI.create(QueueName.prefixForFlow(flowId)));

    DatasetId stateStoreId = getStateStoreId(flowId.getNamespace());
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
            try (Scanner scanner = table.scan(startRow, Bytes.stopKeyForPrefix(startRow))) {
              Row row = scanner.next();
              while (row != null) {
                table.delete(row.getRow());
                row = scanner.next();
              }
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
  public void dropAllInNamespace(NamespaceId namespaceId) throws Exception {
    Set<QueueConstants.QueueType> queueTypes = EnumSet.of(QueueConstants.QueueType.QUEUE,
                                                          QueueConstants.QueueType.SHARDED_QUEUE);

    try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
      for (QueueConstants.QueueType queueType : queueTypes) {
        // Note: The trailing "." is crucial, since otherwise nsId could match nsId1, nsIdx etc
        // It's important to keep config table enabled while disabling and dropping  queue tables.
        final String queueTableNamePrefix = String.format("%s.%s.", NamespaceId.SYSTEM.getNamespace(), queueType);
        final String hbaseNamespace = tableUtil.getHBaseNamespace(namespaceId);
        final TableId configTableId = TableId.from(hbaseNamespace, getConfigTableName());
        tableUtil.deleteAllInNamespace(ddlExecutor, hbaseNamespace, hConf, new Predicate<TableId>() {
          @Override
          public boolean apply(TableId tableId) {
            // It's a bit hacky here since we know how the Dataset System names tables
            return (tableId.getTableName().startsWith(queueTableNamePrefix)) && !tableId.equals(configTableId);
          }
        });
      }
    }

    // Delete the state store in the namespace
    DatasetId id = getStateStoreId(namespaceId.getEntityName());
    if (datasetFramework.hasInstance(id)) {
      datasetFramework.deleteInstance(id);
    }
  }

  public TableId getDataTableId(FlowId flowId) throws IOException {
    return getDataTableId(flowId, type);
  }

  public TableId getDataTableId(QueueName queueName) throws IOException {
    return getDataTableId(queueName, type);
  }

  public TableId getDataTableId(QueueName queueName, QueueConstants.QueueType queueType) throws IOException {
    if (!queueName.isQueue()) {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a queue.");
    }
    return getDataTableId(new FlowId(queueName.getFirstComponent(),
                                     queueName.getSecondComponent(),
                                     queueName.getThirdComponent()),
                          queueType);
  }

  public TableId getDataTableId(FlowId flowId, QueueConstants.QueueType queueType) throws IOException {
    String tableName = String.format("%s.%s.%s.%s", NamespaceId.SYSTEM.getNamespace(), queueType,
                                     flowId.getApplication(), flowId.getEntityName());
    return tableUtil.createHTableId(new NamespaceId(flowId.getNamespace()), tableName);
  }

  private void upgrade(TableId tableId, Properties properties) throws Exception {
    try (AbstractHBaseDataSetAdmin dsAdmin = new QueueDatasetAdmin(tableId, hConf, cConf, tableUtil, properties)) {
      dsAdmin.upgrade();
    }
  }

  /**
   * @param tableId TableId being checked
   * @return true if the given table is the actual table for the queue (opposed to the config table for the queue
   * or tables for things other than queues).
   */
  private boolean isDataTable(TableId tableId) {
    // checks if table is constructed by getDataTableName
    String tableName = tableId.getTableName();
    if (tableName.split("\\.").length <= 3) {
      return false;
    }

    Set<QueueConstants.QueueType> queueTypes = EnumSet.of(QueueConstants.QueueType.QUEUE,
                                                          QueueConstants.QueueType.SHARDED_QUEUE);
    for (QueueConstants.QueueType queueType : queueTypes) {
      String prefix = NamespaceId.SYSTEM.getNamespace() + "." + queueType.toString();
      if (tableName.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private boolean isStateStoreTable(TableId tableId) {
    return tableId.getTableName().equals(getConfigTableName());
  }

  // only used for create & upgrade of data table
  private final class QueueDatasetAdmin extends AbstractHBaseDataSetAdmin {
    private final Properties properties;

    private QueueDatasetAdmin(TableId tableId, Configuration hConf, CConfiguration cConf,
                              HBaseTableUtil tableUtil, Properties properties) {
      super(tableId, hConf, cConf, tableUtil, locationFactory);
      this.properties = properties;
    }

    @Override
    protected CoprocessorJar createCoprocessorJar() throws IOException {
      List<? extends Class<? extends Coprocessor>> coprocessors = getCoprocessors();
      if (coprocessors.isEmpty()) {
        return CoprocessorJar.EMPTY;
      }

      Location jarFile = coprocessorManager.ensureCoprocessorExists();
      return new CoprocessorJar(coprocessors, jarFile);
    }

    @Override
    protected boolean needsUpdate(HTableDescriptor tableDescriptor) {
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
      TableDescriptorBuilder tdBuilder = HBaseTableUtil.getTableDescriptorBuilder(tableId, cConf);
      for (String key : properties.stringPropertyNames()) {
        tdBuilder.addProperty(key, properties.getProperty(key));
      }

      ColumnFamilyDescriptorBuilder cfdBuilder
        = HBaseTableUtil.getColumnFamilyDescriptorBuilder(Bytes.toString(QueueEntryRow.COLUMN_FAMILY), hConf);

      tdBuilder.addColumnFamily(cfdBuilder.build());

      // Add coprocessors
      CoprocessorJar coprocessorJar = createCoprocessorJar();
      for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
        tdBuilder.addCoprocessor(
          coprocessorManager.getCoprocessorDescriptor(coprocessor, coprocessorJar.getPriority(coprocessor)));
      }

      // Create queue table with splits. The distributor bucket size is the same as splits.
      int splits = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS);
      AbstractRowKeyDistributor distributor = new RowKeyDistributorByHashPrefix(
        new RowKeyDistributorByHashPrefix.OneByteSimpleHash(splits));

      byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits, splits, distributor);
      tdBuilder.addProperty(QueueConstants.DISTRIBUTOR_BUCKETS, Integer.toString(splits));
      createQueueTable(tdBuilder, splitKeys);
    }

    private void createQueueTable(TableDescriptorBuilder tdBuilder, byte[][] splitKeys) throws IOException {
      int prefixBytes = (type == QueueConstants.QueueType.SHARDED_QUEUE) ? ShardedHBaseQueueStrategy.PREFIX_BYTES
                                                                         : SaltedHBaseQueueStrategy.SALT_BYTES;
      String prefix = Integer.toString(prefixBytes);
      tdBuilder.addProperty(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES, prefix);
      LOG.info("Create queue table with prefix bytes {}", prefix);

      try (HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get()) {
        ddlExecutor.createTableIfNotExists(tdBuilder.build(), splitKeys);
      }
    }
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  private Map<TableId, Throwable> waitForUpgrade(Map<TableId, Future<?>> upgradeFutures) throws InterruptedException {
    Map<TableId, Throwable> failed = new HashMap<>();
    for (Map.Entry<TableId, Future<?>> entry : upgradeFutures.entrySet()) {
      try {
        entry.getValue().get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException && e.getCause().getCause() != null) {
          failed.put(entry.getKey(), e.getCause().getCause());
        } else {
          failed.put(entry.getKey(), e.getCause());
        }
      }
    }
    return failed;
  }
}
