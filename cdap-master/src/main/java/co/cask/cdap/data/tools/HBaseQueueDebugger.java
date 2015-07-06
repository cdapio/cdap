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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data.runtime.DataSetsModules;
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
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionNotInProgressException;
import co.cask.tephra.runtime.TransactionClientModule;
import co.cask.tephra.runtime.TransactionModules;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.twill.zookeeper.ZKClientService;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Debugging tool for queues in hbase.
 */
public class HBaseQueueDebugger extends AbstractIdleService {

  private final HBaseQueueAdmin queueAdmin;
  private final ZKClientService zkClientService;
  private final HBaseQueueClientFactory queueClientFactory;
  private final TransactionExecutorFactory txExecutorFactory;

  @Inject
  public HBaseQueueDebugger(HBaseQueueAdmin queueAdmin,
                            HBaseQueueClientFactory queueClientFactory,
                            ZKClientService zkClientService,
                            TransactionExecutorFactory txExecutorFactory) {
    this.queueAdmin = queueAdmin;
    this.queueClientFactory = queueClientFactory;
    this.zkClientService = zkClientService;
    this.txExecutorFactory = txExecutorFactory;
  }

  @Override
  protected void startUp() throws Exception {
    zkClientService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    zkClientService.stopAndWait();
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
    System.out.printf("Got %d barriers\n", barriers.size());

    QueueStatistics stats = new QueueStatistics();

    if (consumerGroupId != null) {
      barriers = Multimaps.filterKeys(barriers, Predicates.equalTo(consumerGroupId));
    }

    for (Map.Entry<Long, Collection<QueueBarrier>> entry : barriers.asMap().entrySet()) {
      long groupId = entry.getKey();
      Collection<QueueBarrier> groupBarriers = entry.getValue();

      System.out.printf("Scanning barriers for group %d\n", groupId);

      int currentSection = 1;
      PeekingIterator<QueueBarrier> barrierIterator = Iterators.peekingIterator(groupBarriers.iterator());
      while (barrierIterator.hasNext()) {
        QueueBarrier start = barrierIterator.next();
        QueueBarrier end = barrierIterator.hasNext() ? barrierIterator.peek() : null;

        System.out.printf("Scanning section %d/%d...\n", currentSection, groupBarriers.size());
        scanQueue(txExecutor, stateStore, queueName, start, end, stats);
        System.out.printf("Current results: %s\n", stats.getReport());
        currentSection++;
      }
      System.out.println("Scanning complete");
    }

    System.out.printf("Total results: %s\n", stats.getReport());
    return stats;
  }

  private void scanQueue(TransactionExecutor txExecutor, HBaseConsumerStateStore stateStore,
                         QueueName queueName, QueueBarrier start,
                         @Nullable QueueBarrier end, final QueueStatistics outStats) throws Exception {

    final byte[] queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);

    ConsumerGroupConfig groupConfig = start.getGroupConfig();
    System.out.printf("Got consumer group config: %s\n", groupConfig);

    HBaseQueueAdmin admin = queueClientFactory.getQueueAdmin();
    TableId tableId = admin.getDataTableId(queueName, QueueConstants.QueueType.SHARDED_QUEUE);
    HTable hTable = queueClientFactory.createHTable(tableId);

    System.out.printf("Looking at HBase table: %s\n", Bytes.toString(hTable.getTableName()));

    final byte[] stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX,
                                             Bytes.toBytes(groupConfig.getGroupId()));

    int distributorBuckets = queueClientFactory.getDistributorBuckets(hTable.getTableDescriptor());
    ShardedHBaseQueueStrategy queueStrategy = new ShardedHBaseQueueStrategy(distributorBuckets);

    Scan scan = new Scan();
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

    System.out.printf("Scanning section with scan: %s\n", scan.toString());

    List<Integer> instanceIds = Lists.newArrayList();
    if (groupConfig.getDequeueStrategy() == DequeueStrategy.FIFO) {
      instanceIds.add(0);
    } else {
      for (int instanceId = 0; instanceId < groupConfig.getGroupSize(); instanceId++) {
        instanceIds.add(instanceId);
      }
    }

    final int rowsCache = Integer.parseInt(System.getProperty("rows.cache", "100000"));
    for (final int instanceId : instanceIds) {
      System.out.printf("Processing instance %d", instanceId);
      ConsumerConfig consConfig = new ConsumerConfig(groupConfig, instanceId);
      final QueueScanner scanner = queueStrategy.createScanner(consConfig, hTable, scan, rowsCache);

      try {
        txExecutor.execute(new TransactionExecutor.Procedure<HBaseConsumerStateStore>() {
          @Override
          public void apply(HBaseConsumerStateStore input) throws Exception {
            ImmutablePair<byte[], Map<byte[], byte[]>> result;
            while ((result = scanner.next()) != null) {
              byte[] rowKey = result.getFirst();
              Map<byte[], byte[]> columns = result.getSecond();
              visitRow(outStats, input.getTransaction(), rowKey, columns.get(stateColumnName), queueRowPrefix.length);

              if (Boolean.parseBoolean(System.getProperty("show.progress")) && outStats.getTotal() % rowsCache == 0) {
                System.out.printf("\rProcessing instance %d: %s", instanceId, outStats.getReport());
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
      System.out.printf("\rProcessing instance %d: %s\n", instanceId, outStats.getReport());
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

    private long unprocessed;
    private long processedAndVisible;
    private long processedAndNotVisible;

    private QueueStatistics() {
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

    public String getReport() {
      return String.format("unprocessed: %d; processed and visible: %d; processed and not visible: %d; total: %d",
                           getUnprocessed(), getProcessedAndVisible(), getProcessedAndNotVisible(), getTotal());
    }
  }

  public static void main(String[] args) throws Exception {
    // TODO: Use commons-cli for parsing command-line args
    if (args.length == 0) {
      System.out.println("Expected arguments: <queue-uri> [consumer-flowlet]");
      System.out.println("queue-uri: queue:///<namespace>/<app>/<flow>/<flowlet>/<queue>");
      System.out.println("consumer-flowlet: <flowlet>");
      System.out.println("Example: queue:///default/PurchaseHistory/PurchaseFlow/reader/queue collector");
      System.out.println();
      System.out.println("System properties:");
      System.out.println("-Dshow.progress=true         Show progress while scanning the queue table");
      System.out.println("-Drows.cache=[num_of_rows]   Number of rows to pass to HBase Scan.setCaching() method");
      System.exit(1);
    }

    // e.g. "queue:///default/PurchaseHistory/PurchaseFlow/reader/queue"
    final QueueName queueName = QueueName.from(URI.create(args[0]));
    Long consumerGroupId = null;
    if (args.length >= 2) {
      String consumerFlowlet = args[1];
      Id.Program flowId = Id.Program.from(queueName.getFirstComponent(), queueName.getSecondComponent(),
                                          ProgramType.FLOW, queueName.getThirdComponent());
      consumerGroupId = FlowUtils.generateConsumerGroupId(flowId, consumerFlowlet);
    }

    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new ZKClientModule(),
      new TransactionClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
          bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
        }
      },
      new DiscoveryRuntimeModule().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new TransactionModules().getDistributedModules()
    );

    HBaseQueueDebugger debugger = injector.getInstance(HBaseQueueDebugger.class);
    debugger.startAndWait();
    debugger.scanQueue(queueName, consumerGroupId);
    debugger.stopAndWait();
  }
}
