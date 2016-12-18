/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.guice.TwillModule;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.ConsumerEntryState;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueScanner;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseConsumerStateStore;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import co.cask.cdap.data2.transaction.queue.hbase.QueueBarrier;
import co.cask.cdap.data2.transaction.queue.hbase.ShardedHBaseQueueStrategy;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.queue.SimpleQueueSpecificationGenerator;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TxConstants;
import org.apache.twill.zookeeper.ZKClientService;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Debugging tool for queues in hbase.
 */
public class HBaseQueueDebugger extends AbstractIdleService {

  public static final String PROP_SHOW_TX_TIMESTAMP_ONLY = "show.tx.timestamp.only";
  public static final String PROP_SHOW_PROGRESS = "show.progress";
  public static final String PROP_ROWS_CACHE = "rows.cache";

  private final HBaseTableUtil tableUtil;
  private final HBaseQueueAdmin queueAdmin;
  private final ZKClientService zkClientService;
  private final HBaseQueueClientFactory queueClientFactory;
  private final TransactionExecutorFactory txExecutorFactory;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Store store;
  private final Impersonator impersonator;
  private final AuthorizationEnforcementService authorizationEnforcementService;

  @Inject
  public HBaseQueueDebugger(HBaseTableUtil tableUtil, HBaseQueueAdmin queueAdmin,
                            HBaseQueueClientFactory queueClientFactory,
                            ZKClientService zkClientService,
                            TransactionExecutorFactory txExecutorFactory,
                            NamespaceQueryAdmin namespaceQueryAdmin,
                            Store store, Impersonator impersonator,
                            AuthorizationEnforcementService authorizationEnforcementService) {
    this.tableUtil = tableUtil;
    this.queueAdmin = queueAdmin;
    this.queueClientFactory = queueClientFactory;
    this.zkClientService = zkClientService;
    this.txExecutorFactory = txExecutorFactory;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.store = store;
    this.impersonator = impersonator;
    this.authorizationEnforcementService = authorizationEnforcementService;
  }

  @Override
  protected void startUp() throws Exception {
    zkClientService.startAndWait();
    authorizationEnforcementService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    authorizationEnforcementService.stopAndWait();
    zkClientService.stopAndWait();
  }

  public void scanAllQueues() throws Exception {
    final QueueStatistics totalStats = new QueueStatistics();

    List<NamespaceMeta> namespaceMetas = namespaceQueryAdmin.list();
    for (final NamespaceMeta namespaceMeta : namespaceMetas) {
      final Collection<ApplicationSpecification> apps = store.getAllApplications(namespaceMeta.getNamespaceId());
      impersonator.doAs(namespaceMeta, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          for (ApplicationSpecification app : apps) {
            ApplicationId appId = new ApplicationId(namespaceMeta.getName(), app.getName(), app.getAppVersion());

            for (FlowSpecification flow : app.getFlows().values()) {
              ProgramId flowId = appId.program(ProgramType.FLOW, flow.getName());

              SimpleQueueSpecificationGenerator queueSpecGenerator =
                new SimpleQueueSpecificationGenerator(flowId.getParent());

              Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> table =
                queueSpecGenerator.create(flow);
              for (Table.Cell<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> cell
                : table.cellSet()) {
                if (cell.getRowKey().getType() == FlowletConnection.Type.FLOWLET) {
                  for (QueueSpecification queue : cell.getValue()) {
                    QueueStatistics queueStats = scanQueue(queue.getQueueName(), null);
                    totalStats.add(queueStats);
                  }
                }
              }
            }
          }
          return null;
        }
      });

    }

    System.out.printf("Total results for all queues: %s\n", totalStats.getReport(showTxTimestampOnly()));
  }

  /**
   * Only works for {@link co.cask.cdap.data2.transaction.queue.hbase.ShardedHBaseQueueStrategy}.
   */
  public QueueStatistics scanQueue(final QueueName queueName, @Nullable Long consumerGroupId) throws Exception {
    HBaseConsumerStateStore stateStore;
    try {
      stateStore = queueAdmin.getConsumerStateStore(queueName);
    } catch (IllegalStateException e) {
      throw new NotFoundException(queueName);
    }

    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, stateStore);
    Multimap<Long, QueueBarrier> barriers = txExecutor.execute(
      new TransactionExecutor.Function<HBaseConsumerStateStore, Multimap<Long, QueueBarrier>>() {
        @Override
        public Multimap<Long, QueueBarrier> apply(HBaseConsumerStateStore input) throws Exception {
          return input.getAllBarriers();
        }
      }, stateStore);
    printProgress("Got %d barriers\n", barriers.size());

    QueueStatistics stats = new QueueStatistics();

    if (consumerGroupId != null) {
      barriers = Multimaps.filterKeys(barriers, Predicates.equalTo(consumerGroupId));
    }

    for (Map.Entry<Long, Collection<QueueBarrier>> entry : barriers.asMap().entrySet()) {
      long groupId = entry.getKey();
      Collection<QueueBarrier> groupBarriers = entry.getValue();

      printProgress("Scanning barriers for group %d\n", groupId);

      int currentSection = 1;
      PeekingIterator<QueueBarrier> barrierIterator = Iterators.peekingIterator(groupBarriers.iterator());
      while (barrierIterator.hasNext()) {
        QueueBarrier start = barrierIterator.next();
        QueueBarrier end = barrierIterator.hasNext() ? barrierIterator.peek() : null;

        printProgress("Scanning section %d/%d...\n", currentSection, groupBarriers.size());
        scanQueue(txExecutor, stateStore, queueName, start, end, stats);
        printProgress("Current results: %s\n", stats.getReport(showTxTimestampOnly()));
        currentSection++;
      }
      printProgress("Scanning complete");
    }

    System.out.printf("Results for queue %s: %s\n",
                      queueName.toString(), stats.getReport(showTxTimestampOnly()));
    return stats;
  }

  private void printProgress(String format, Object... args) {
    if (showProgress()) {
      System.out.printf(format, args);
    }
  }

  private boolean showTxTimestampOnly() {
    return Boolean.parseBoolean(System.getProperty(PROP_SHOW_TX_TIMESTAMP_ONLY));
  }

  private boolean showProgress() {
    return Boolean.parseBoolean(System.getProperty(PROP_SHOW_PROGRESS));
  }

  private void scanQueue(TransactionExecutor txExecutor, HBaseConsumerStateStore stateStore,
                         QueueName queueName, QueueBarrier start,
                         @Nullable QueueBarrier end, final QueueStatistics outStats) throws Exception {

    final byte[] queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);

    ConsumerGroupConfig groupConfig = start.getGroupConfig();
    printProgress("Got consumer group config: %s\n", groupConfig);

    HBaseQueueAdmin admin = queueClientFactory.getQueueAdmin();
    TableId tableId = admin.getDataTableId(queueName, QueueConstants.QueueType.SHARDED_QUEUE);
    HTable hTable = queueClientFactory.createHTable(tableId);

    printProgress("Looking at HBase table: %s\n", Bytes.toString(hTable.getTableName()));

    final byte[] stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX,
                                             Bytes.toBytes(groupConfig.getGroupId()));

    int distributorBuckets = queueClientFactory.getDistributorBuckets(hTable.getTableDescriptor());
    ShardedHBaseQueueStrategy queueStrategy = new ShardedHBaseQueueStrategy(tableUtil, distributorBuckets);

    ScanBuilder scan = tableUtil.buildScan();
    scan.setStartRow(start.getStartRow());
    if (end != null) {
      scan.setStopRow(end.getStartRow());
    } else {
      scan.setStopRow(QueueEntryRow.getQueueEntryRowKey(queueName, Long.MAX_VALUE, Integer.MAX_VALUE));
    }

    // Needs to include meta column for row that doesn't have state yet.
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, QueueEntryRow.META_COLUMN);
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
    // Don't do block cache for debug tool. We don't want old blocks get cached
    scan.setCacheBlocks(false);
    scan.setMaxVersions(1);

    printProgress("Scanning section with scan: %s\n", scan.toString());

    List<Integer> instanceIds = Lists.newArrayList();
    if (groupConfig.getDequeueStrategy() == DequeueStrategy.FIFO) {
      instanceIds.add(0);
    } else {
      for (int instanceId = 0; instanceId < groupConfig.getGroupSize(); instanceId++) {
        instanceIds.add(instanceId);
      }
    }

    final int rowsCache = Integer.parseInt(System.getProperty(PROP_ROWS_CACHE, "100000"));
    for (final int instanceId : instanceIds) {
      printProgress("Processing instance %d", instanceId);
      ConsumerConfig consConfig = new ConsumerConfig(groupConfig, instanceId);
      final QueueScanner scanner = queueStrategy.createScanner(consConfig, hTable, scan.build(), rowsCache);

      try {
        txExecutor.execute(new TransactionExecutor.Procedure<HBaseConsumerStateStore>() {
          @Override
          public void apply(HBaseConsumerStateStore input) throws Exception {
            ImmutablePair<byte[], Map<byte[], byte[]>> result;
            while ((result = scanner.next()) != null) {
              byte[] rowKey = result.getFirst();
              Map<byte[], byte[]> columns = result.getSecond();
              visitRow(outStats, input.getTransaction(), rowKey, columns.get(stateColumnName), queueRowPrefix.length);

              if (showProgress() && outStats.getTotal() % rowsCache == 0) {
                System.out.printf("\rProcessing instance %d: %s",
                                  instanceId, outStats.getReport(showTxTimestampOnly()));
              }
            }
          }
        }, stateStore);
      } catch (TransactionFailureException e) {
        // Ignore transaction not in progress exception as it's caued by short TX timeout on commit
        if (!(Throwables.getRootCause(e) instanceof TransactionNotInProgressException)) {
          throw Throwables.propagate(e);
        }
      }
      printProgress("\rProcessing instance %d: %s\n", instanceId, outStats.getReport(showTxTimestampOnly()));
    }
  }

  /**
   * @param tx the transaction
   * @param rowKey the key of the row
   * @param stateValue the value of the state column in the row
   * @param queueRowPrefixLength length of the queueRowPrefix
   */
  private void visitRow(QueueStatistics stats, Transaction tx, byte[] rowKey,
                        byte[] stateValue, int queueRowPrefixLength) {
    if (stateValue == null) {
      stats.countUnprocessed(1);
      return;
    }

    ConsumerEntryState state = QueueEntryRow.getState(stateValue);
    if (state == ConsumerEntryState.PROCESSED) {
      long writePointer = QueueEntryRow.getWritePointer(rowKey, queueRowPrefixLength);
      stats.recordMinWritePointer(writePointer);
      if (tx.isVisible(writePointer)) {
        stats.countProcessedAndVisible(1);
      } else {
        stats.countProcessedAndNotVisible(1);
      }
    }
  }

  /**
   *
   */
  public static final class QueueStatistics {

    private Optional<Long> minWritePointer = Optional.absent();
    private long unprocessed;
    private long processedAndVisible;
    private long processedAndNotVisible;

    private QueueStatistics() {
    }

    public void recordMinWritePointer(long writePointer) {
      if (minWritePointer.isPresent()) {
        this.minWritePointer = Optional.of(Math.min(minWritePointer.get(), writePointer));
      } else {
        this.minWritePointer = Optional.of(writePointer);
      }
    }

    public void countUnprocessed(long count) {
      unprocessed += count;
    }

    public void countProcessedAndVisible(long count) {
      processedAndVisible += count;
    }

    public void countProcessedAndNotVisible(long count) {
      processedAndNotVisible += count;
    }

    public long getUnprocessed() {
      return unprocessed;
    }

    public long getProcessedAndVisible() {
      return processedAndVisible;
    }

    public long getProcessedAndNotVisible() {
      return processedAndNotVisible;
    }

    public long getTotal() {
      return unprocessed + processedAndVisible + processedAndNotVisible;
    }

    public Optional<Long> getMinWritePointer() {
      return minWritePointer;
    }

    public String getMinWritePointerString() {
      if (minWritePointer.isPresent()) {
        return Long.toString(minWritePointer.get());
      } else {
        return "n/a";
      }
    }

    public String getMinWritePointerTimestampString() {
      if (minWritePointer.isPresent()) {
        return Long.toString(minWritePointer.get() / TxConstants.MAX_TX_PER_MS);
      } else {
        return "n/a";
      }
    }

    private String getTxTimestampReport() {
      return String.format("min tx timestamp: %s", getMinWritePointerTimestampString());
    }

    private String getDetailedReport() {
      return String.format("min write pointer: %s; unprocessed: %d; processed and visible: %d; " +
                             "processed and not visible: %d; total: %d",
                           getMinWritePointerString(), getUnprocessed(), getProcessedAndVisible(),
                           getProcessedAndNotVisible(), getTotal());
    }

    public String getReport(boolean showTxTimestampOnly) {
      if (showTxTimestampOnly) {
        return getTxTimestampReport();
      } else {
        return getDetailedReport();
      }
    }

    public void add(QueueStatistics stats) {
      if (stats.getMinWritePointer().isPresent()) {
        recordMinWritePointer(stats.getMinWritePointer().get());
      }
      countUnprocessed(stats.getUnprocessed());
      countProcessedAndNotVisible(stats.getProcessedAndNotVisible());
      countProcessedAndVisible(stats.getProcessedAndVisible());
    }
  }

  public static HBaseQueueDebugger createDebugger() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    if (cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      System.out.println(String.format("Disabling authorization for %s.", HBaseQueueDebugger.class.getSimpleName()));
      cConf.setBoolean(Constants.Security.Authorization.ENABLED, false);
    }
    // Note: login has to happen before any objects that need Kerberos credentials are instantiated.
    SecurityUtil.loginForMasterService(cConf);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, HBaseConfiguration.create()),
      new IOModule(),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new TwillModule(),
      new ExploreClientModule(),
      new DataFabricModules().getDistributedModules(),
      new ServiceStoreModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new KafkaClientModule(),
      new NamespaceStoreModule().getDistributedModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new SecureStoreModules().getDistributedModules(),
      new MessagingClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
          bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
          bind(Store.class).annotatedWith(Names.named("defaultStore")).to(DefaultStore.class).in(Singleton.class);

          // This is needed because the LocalApplicationManager
          // expects a dsframework injection named datasetMDS
          bind(DatasetFramework.class)
            .annotatedWith(Names.named("datasetMDS"))
            .to(DatasetFramework.class).in(Singleton.class);
        }
      });

    return injector.getInstance(HBaseQueueDebugger.class);
  }

  public static void main(String[] args) throws Exception {
    if (args.length >= 1 && args[0].equals("help")) {
      System.out.println("Arguments: [<queue-uri> [consumer-flowlet]]");
      System.out.println("queue-uri: queue:///<namespace>/<app>/<flow>/<flowlet>/<queue>");
      System.out.println("consumer-flowlet: <flowlet>");
      System.out.println("If queue-uri is not provided, scan all queues");
      System.out.println("Example: queue:///default/PurchaseHistory/PurchaseFlow/reader/queue collector");
      System.out.println();
      System.out.println("System properties:");
      System.out.println("-D" + PROP_SHOW_PROGRESS + "=true         Show progress while scanning the queue table");
      System.out.println("-D" + PROP_ROWS_CACHE + "=[num_of_rows]   " +
                         "Number of rows to pass to HBase Scan.setCaching() method");
      System.exit(1);
    }

    // e.g. "queue:///default/PurchaseHistory/PurchaseFlow/reader/queue"
    final QueueName queueName = args.length >= 1 ? QueueName.from(URI.create(args[0])) : null;
    Long consumerGroupId = null;
    if (args.length >= 2) {
      Preconditions.checkNotNull(queueName);
      String consumerFlowlet = args[1];
      FlowId flowId = new FlowId(queueName.getFirstComponent(), queueName.getSecondComponent(),
                                 queueName.getThirdComponent());
      consumerGroupId = FlowUtils.generateConsumerGroupId(flowId, consumerFlowlet);
    }

    HBaseQueueDebugger debugger = createDebugger();
    debugger.startAndWait();
    if (queueName != null) {
      debugger.scanQueue(queueName, consumerGroupId);
    } else {
      debugger.scanAllQueues();
    }
    debugger.stopAndWait();
  }
}
