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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.ThriftClientProviderSupplier;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.queue.AbstractQueueConsumer;
import co.cask.cdap.data2.transaction.queue.ConsumerEntryState;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.tephra.Transaction;
import co.cask.tephra.distributed.ThriftClientProvider;
import co.cask.tephra.distributed.TransactionServiceClient;
import co.cask.tephra.runtime.TransactionModules;
import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * Debugging tool for queues in hbase.
 */
public class HBaseQueueDebugger extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueDebugger.class);
  private static final Gson GSON = new Gson();

  private final HBaseQueueAdmin queueAdmin;
  private final TransactionServiceClient txClient;
  private final ZKClientService zkClientService;
  private final HBaseQueueClientFactory queueClientFactory;

  @Inject
  public HBaseQueueDebugger(HBaseQueueAdmin queueAdmin,
                            HBaseQueueClientFactory queueClientFactory,
                            TransactionServiceClient txClient,
                            ZKClientService zkClientService) {
    this.queueAdmin = queueAdmin;
    this.queueClientFactory = queueClientFactory;
    this.txClient = txClient;
    this.zkClientService = zkClientService;
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
  private void scanQueue(URI queueNameURI) throws Exception {
    QueueName queueName = QueueName.from(queueNameURI);
    HBaseConsumerStateStore stateStore = queueAdmin.getConsumerStateStore(queueName);

    Transaction tx = txClient.startLong();
    stateStore.startTx(tx);

    Multimap<Long, QueueBarrier> barriers = stateStore.getAllBarriers();
    LOG.info("Got {} barriers", barriers.size());

    QueueStatistics stats = new QueueStatistics();
    int currentSection = 1;
    for (Long groupId : barriers.keySet()) {
      LOG.info("Scanning barriers for group {}", groupId);
      Collection<QueueBarrier> groupBarriers = barriers.get(groupId);
      Iterator<QueueBarrier> barrierIterator = groupBarriers.iterator();

      QueueBarrier previous;
      QueueBarrier current = null;
      while (barrierIterator.hasNext()) {
        previous = current;
        current = barrierIterator.next();

        if (previous != null) {
          // after first iteration
          LOG.info("Scanning section {}/{}...", currentSection, barriers.size());
          QueueStatistics sectionStats = scanQueue(tx, queueName, previous, current);
          LOG.info("Section results: {}", sectionStats.getReport());

          stats = QueueStatistics.of(stats, sectionStats);
          currentSection++;
        }
      }
      // handle last barrier
      LOG.info("Scanning section {}/{}...", currentSection, barriers.size());
      stats = QueueStatistics.of(stats, scanQueue(tx, queueName, current, null));
      LOG.info("Scanning complete", currentSection, barriers.size());
    }

    LOG.info("Total results: {}", stats.getReport());
    stateStore.commitTx();
  }

  private QueueStatistics scanQueue(Transaction tx, QueueName queueName, QueueBarrier start,
                                    @Nullable QueueBarrier end) throws IOException {

    byte[] queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);

    ConsumerGroupConfig consumerConfig = start.getGroupConfig();
    LOG.info("Got consumer group config: {}", consumerConfig);

    HBaseQueueAdmin admin = queueClientFactory.ensureTableExists(queueName);
    TableId tableId = admin.getDataTableId(queueName, QueueConstants.QueueType.SHARDED_QUEUE);
    HTable hTable = queueClientFactory.createHTable(tableId);

    LOG.info("Looking at HBase table: {}", Bytes.toString(hTable.getTableName()));

    byte[] stateColumnName = Bytes.add(QueueEntryRow.STATE_COLUMN_PREFIX,
                                       Bytes.toBytes(consumerConfig.getGroupId()));


    int distributorBuckets = queueClientFactory.getDistributorBuckets(hTable.getTableDescriptor());
    ShardedHBaseQueueStrategy queueStrategy = new ShardedHBaseQueueStrategy(distributorBuckets);

    Scan scan = new Scan();
    scan.setStartRow(start.getStartRow());
    if (end != null) {
      scan.setStopRow(end.getStartRow());
    }
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, QueueEntryRow.DATA_COLUMN);
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, QueueEntryRow.META_COLUMN);
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
    scan.setCaching(100);
    scan.setMaxVersions(1);

    LOG.info("Scanning section with scan: {}", GSON.toJson(scan));
    QueueStatistics stats = new QueueStatistics();

    ConsumerConfig consConfig = new ConsumerConfig(consumerConfig, 0);
    ResultScanner scanner = queueStrategy.createHBaseScanner(consConfig, hTable, scan, 100, true);

    Function<byte[], byte[]> rowKeyConverter = ShardedHBaseQueueStrategy.ROW_KEY_CONVERTER;

    Result result;
    while ((result = scanner.next()) != null) {
      NavigableMap<byte[], byte[]> columns = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
      byte[] rowKey = rowKeyConverter.apply(result.getRow());
      visitRow(stats, tx, rowKey, columns.get(stateColumnName), queueRowPrefix.length);
    }
    return stats;
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
      stats.countUnprocessed();
      return;
    }

    ConsumerEntryState state = QueueEntryRow.getState(stateValue);
    int stateInstanceId = QueueEntryRow.getStateInstanceId(stateValue);
    if (state == ConsumerEntryState.PROCESSED) {
      long writePointer = AbstractQueueConsumer.getWritePointer(rowKey, queueRowPrefixLength);
      if (tx.isVisible(writePointer)) {
        stats.countProcessedAndVisible();
      } else {
        stats.countProcessedAndNotVisible();
      }
    }
  }

  /**
   *
   */
  private static final class QueueStatistics {

    private long unprocessed;
    private long processedAndVisible;
    private long processedAndNotVisible;

    private QueueStatistics() {
    }

    private QueueStatistics(long unprocessed, long processedAndVisible, long processedAndNotVisible) {
      this.unprocessed = unprocessed;
      this.processedAndVisible = processedAndVisible;
      this.processedAndNotVisible = processedAndNotVisible;
    }

    public static QueueStatistics of(QueueStatistics...stats) {
      QueueStatistics result = new QueueStatistics();
      for (QueueStatistics stat : stats) {
        result.unprocessed += stat.unprocessed;
        result.processedAndVisible += stat.processedAndVisible;
        result.processedAndNotVisible += stat.processedAndNotVisible;
      }
      return result;
    }

    public void countUnprocessed() {
      unprocessed++;
    }

    public void countProcessedAndVisible() {
      processedAndVisible++;
    }

    public void countProcessedAndNotVisible() {
      processedAndNotVisible++;
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
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new ZKClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ThriftClientProvider.class).toProvider(ThriftClientProviderSupplier.class);
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

    // e.g. "queue:///default/PurchaseHistory/PurchaseFlow/reader/queue"
    String queueNameURI = args[0];
    debugger.scanQueue(URI.create(queueNameURI));

    debugger.stopAndWait();
  }
}
