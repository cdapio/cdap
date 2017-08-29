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
package co.cask.cdap.data2.transaction.queue.coprocessor.hbase98;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.cdap.data2.transaction.queue.ConsumerEntryState;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.SaltedHBaseQueueStrategy;
import co.cask.cdap.data2.util.hbase.CConfigurationReader;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.ConsumerConfigCache;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.ConsumerConfigCacheSupplier;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.ConsumerInstance;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.QueueConsumerConfig;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.TableNameAwareCacheSupplier;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import com.google.common.base.Supplier;
import com.google.common.io.InputSupplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.CacheSupplier;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.hbase.txprune.CompactionState;
import org.apache.tephra.persist.TransactionVisibilityState;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * RegionObserver for queue table. This class should only have JSE and HBase classes dependencies only.
 * It can also has dependencies on CDAP classes provided that all the transitive dependencies stay within
 * the mentioned scope.
 *
 * This region observer does queue eviction during flush time and compact time by using queue consumer state
 * information to determine if a queue entry row can be omitted during flush/compact.
 */
public final class HBaseQueueRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(HBaseQueueRegionObserver.class);

  private TableName configTableName;
  private CConfigurationReader cConfReader;
  private Supplier<TransactionVisibilityState> txSnapshotSupplier;

  private CacheSupplier<TransactionStateCache> txStateCacheSupplier;
  private ConsumerConfigCacheSupplier configCacheSupplier;
  private CompactionState compactionState;

  private TransactionStateCache txStateCache;
  private ConsumerConfigCache configCache;
  private Boolean pruneEnable;

  private int prefixBytes;
  private String namespaceId;
  private String appName;
  private String flowName;

  @Override
  public void start(CoprocessorEnvironment env) {
    if (env instanceof RegionCoprocessorEnvironment) {
      HTableDescriptor tableDesc = ((RegionCoprocessorEnvironment) env).getRegion().getTableDesc();
      String hTableName = tableDesc.getNameAsString();

      String prefixBytes = tableDesc.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES);
      try {
        // Default to SALT_BYTES for the older salted queue implementation.
        this.prefixBytes = prefixBytes == null ? SaltedHBaseQueueStrategy.SALT_BYTES : Integer.parseInt(prefixBytes);
      } catch (NumberFormatException e) {
        // Shouldn't happen for table created by cdap.
        LOG.error("Unable to parse value of '" + HBaseQueueAdmin.PROPERTY_PREFIX_BYTES + "' property. " +
                    "Default to " + SaltedHBaseQueueStrategy.SALT_BYTES, e);
        this.prefixBytes = SaltedHBaseQueueStrategy.SALT_BYTES;
      }

      namespaceId = HTableNameConverter.from(tableDesc).getNamespace();
      appName = HBaseQueueAdmin.getApplicationName(hTableName);
      flowName = HBaseQueueAdmin.getFlowName(hTableName);

      Configuration conf = env.getConfiguration();
      String tablePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);
      txStateCacheSupplier = new DefaultTransactionStateCacheSupplier(tablePrefix, env);
      txStateCache = txStateCacheSupplier.get();
      txSnapshotSupplier = new Supplier<TransactionVisibilityState>() {
        @Override
        public TransactionVisibilityState get() {
          return txStateCache.getLatestState();
        }
      };
      String queueConfigTableId = HBaseQueueAdmin.getConfigTableName();
      configTableName = HTableNameConverter.toTableName(tablePrefix, TableId.from(namespaceId, queueConfigTableId));
      cConfReader = new CConfigurationReader(env, tablePrefix);
      configCacheSupplier = createConfigCache(env);
      configCache = configCacheSupplier.get();
    }
  }

  @Override
  public void stop(CoprocessorEnvironment e) {
    if (compactionState != null) {
      compactionState.stop();
    }
    configCacheSupplier.release();
    txStateCacheSupplier.release();
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e,
                                  Store store, InternalScanner scanner) throws IOException {
    if (!e.getEnvironment().getRegion().isAvailable()) {
      return scanner;
    }

    LOG.info("preFlush, creates EvictionInternalScanner");
    return new EvictionInternalScanner("flush", e.getEnvironment(), scanner, null);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                    InternalScanner scanner, ScanType type,
                                    CompactionRequest request) throws IOException {
    if (!e.getEnvironment().getRegion().isAvailable()) {
      return scanner;
    }

    LOG.info("preCompact, creates EvictionInternalScanner");
    TransactionVisibilityState txVisibilityState = txStateCache.getLatestState();

    reloadPruneState(e.getEnvironment());
    if (compactionState != null) {
      // Record tx state before the compaction
      compactionState.record(request, txVisibilityState);
    }

    return new EvictionInternalScanner("compaction", e.getEnvironment(), scanner, txVisibilityState);
  }

  @Override
  public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile,
                          CompactionRequest request) throws IOException {
    // Persist the compaction state after a successful compaction
    if (this.compactionState != null) {
      this.compactionState.persist();
    }
  }

  @Override
  public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    // Record whether the region is empty after a flush
    HRegion region = e.getEnvironment().getRegion();
    // After a flush, if the memstore size is zero and there are no store files for any stores in the region
    // then the region must be empty
    long numStoreFiles = numStoreFilesForRegion(e);
    long memstoreSize = region.getMemstoreSize().get();
    LOG.debug(String.format("Region %s: memstore size = %s, num store files = %s",
                            region.getRegionInfo().getRegionNameAsString(), memstoreSize, numStoreFiles));
    if (memstoreSize == 0 && numStoreFiles == 0) {
      if (compactionState != null) {
        compactionState.persistRegionEmpty(System.currentTimeMillis());
      }
    }
  }

  private long numStoreFilesForRegion(ObserverContext<RegionCoprocessorEnvironment> c) {
    long numStoreFiles = 0;
    for (Store store : c.getEnvironment().getRegion().getStores().values()) {
      numStoreFiles += store.getStorefiles().size();
    }
    return numStoreFiles;
  }

  private void reloadPruneState(RegionCoprocessorEnvironment env) {
    if (pruneEnable == null) {
      // If prune enable has never been initialized, try to do so now
      initializePruneState(env);
    } else {
      CConfiguration conf = configCache.getCConf();
      if (conf != null) {
        boolean newPruneEnable = conf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE,
                                                 TxConstants.TransactionPruning.DEFAULT_PRUNE_ENABLE);
        if (newPruneEnable != pruneEnable) {
          // pruning enable has been changed, resetting prune state
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Transaction Invalid List pruning feature is set to %s now for region %s.",
                                    newPruneEnable, env.getRegion().getRegionInfo().getRegionNameAsString()));
          }
          resetPruneState();
          initializePruneState(env);
        }
      }
    }
  }

  private void initializePruneState(RegionCoprocessorEnvironment env) {
    CConfiguration conf = configCache.getCConf();
    if (conf != null) {
      pruneEnable = conf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE,
                                    TxConstants.TransactionPruning.DEFAULT_PRUNE_ENABLE);

      if (Boolean.TRUE.equals(pruneEnable)) {
        String pruneTable = conf.get(TxConstants.TransactionPruning.PRUNE_STATE_TABLE,
                                     TxConstants.TransactionPruning.DEFAULT_PRUNE_STATE_TABLE);
        long pruneFlushInterval = TimeUnit.SECONDS.toMillis(conf.getLong(
          TxConstants.TransactionPruning.PRUNE_FLUSH_INTERVAL,
          TxConstants.TransactionPruning.DEFAULT_PRUNE_FLUSH_INTERVAL));

        compactionState = new CompactionState(env, TableName.valueOf(pruneTable), pruneFlushInterval);
        if (LOG.isDebugEnabled()) {
          TableName tableName = env.getRegion().getRegionInfo().getTable();
          LOG.debug(String.format("Automatic invalid list pruning is enabled for table %s:%s. Compaction state " +
                                    "will be recorded in table %s", tableName.getNamespaceAsString(),
                                  tableName.getNameAsString(), pruneTable));
        }
      }
    }
  }

  private void resetPruneState() {
    pruneEnable = false;
    if (compactionState != null) {
      compactionState.stop();
      compactionState = null;
    }
  }

  // needed for queue unit-test
  @SuppressWarnings("unused")
  private void updateCache() throws IOException {
    if (configCache != null) {
      configCache.updateCache();
    }
  }

  private ConsumerConfigCacheSupplier createConfigCache(final CoprocessorEnvironment env) {
    return TableNameAwareCacheSupplier.getSupplier(configTableName, cConfReader,
                                                   txSnapshotSupplier, new InputSupplier<HTableInterface>() {
        @Override
        public HTableInterface getInput() throws IOException {
          return env.getTable(configTableName);
        }
      });
  }

  // need for queue unit-test
  private TransactionStateCache getTxStateCache() {
    return txStateCache;
  }

  /**
   * An {@link InternalScanner} that will skip queue entries that are safe to be evicted.
   */
  private final class EvictionInternalScanner implements InternalScanner {

    private final String triggeringAction;
    private final RegionCoprocessorEnvironment env;
    private final InternalScanner scanner;
    private final TransactionVisibilityState state;
    // This is just for object reused to reduce objects creation.
    private final ConsumerInstance consumerInstance;
    private byte[] currentQueue;
    private byte[] currentQueueRowPrefix;
    private QueueConsumerConfig consumerConfig;
    private long totalRows = 0;
    private long rowsEvicted = 0;
    // couldn't be evicted due to incomplete view of row
    private long skippedIncomplete = 0;
    private boolean invalidTxData;

    private EvictionInternalScanner(String action, RegionCoprocessorEnvironment env, InternalScanner scanner,
                                    @Nullable TransactionVisibilityState state) {
      this.triggeringAction = action;
      this.env = env;
      this.scanner = scanner;
      this.state = state;
      this.consumerInstance = new ConsumerInstance(0, 0);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      return next(results, -1);
    }

    @Override
    public boolean next(List<Cell> results, int limit) throws IOException {
      boolean hasNext = scanner.next(results, limit);

      while (!results.isEmpty()) {
        totalRows++;
        // Check if it is eligible for eviction.
        Cell cell = results.get(0);

        // If current queue is unknown or the row is not a queue entry of current queue,
        // it either because it scans into next queue entry or simply current queue is not known.
        // Hence needs to find the currentQueue
        if (currentQueue == null || !QueueEntryRow.isQueueEntry(currentQueueRowPrefix, prefixBytes, cell.getRowArray(),
                                                                cell.getRowOffset(), cell.getRowLength())) {
          // If not eligible, it either because it scans into next queue entry or simply current queue is not known.
          currentQueue = null;
        }

        // This row is a queue entry. If currentQueue is null, meaning it's a new queue encountered during scan.
        if (currentQueue == null) {
          QueueName queueName = QueueEntryRow.getQueueName(namespaceId, appName, flowName, prefixBytes,
                                                           cell.getRowArray(), cell.getRowOffset(),
                                                           cell.getRowLength());
          currentQueue = queueName.toBytes();
          currentQueueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);
          consumerConfig = configCache.getConsumerConfig(currentQueue);
        }

        invalidTxData = false;
        if (state != null) {
          long txId = QueueEntryRow.getWritePointer(cell.getRowArray(),
                                                    cell.getRowOffset() + prefixBytes + currentQueueRowPrefix.length);
          if (txId > 0 && state.getInvalid().contains(txId)) {
            invalidTxData = true;
          }
        }

        if (consumerConfig == null && !invalidTxData) {
          // no config is present yet and not invalid data, so cannot evict
          return hasNext;
        }

        if (invalidTxData || canEvict(consumerConfig, results)) {
          rowsEvicted++;
          results.clear();
          hasNext = scanner.next(results, limit);
        } else {
          break;
        }
      }

      return hasNext;
    }

    @Override
    public void close() throws IOException {
      LOG.info("Region " + env.getRegion().getRegionNameAsString() + " " + triggeringAction +
                 ", rows evicted: " + rowsEvicted + " / " + totalRows + ", skipped incomplete: " + skippedIncomplete);
      scanner.close();
    }

    /**
     * Determines the given queue entry row can be evicted.
     * @param result All KeyValues of a queue entry row.
     * @return true if it can be evicted, false otherwise.
     */
    private boolean canEvict(QueueConsumerConfig consumerConfig, List<Cell> result) {
      // If no consumer group, this queue is dead, should be ok to evict.
      if (consumerConfig.getNumGroups() == 0) {
        return true;
      }

      // If unknown consumer config (due to error), keep the queue.
      if (consumerConfig.getNumGroups() < 0) {
        return false;
      }

      // TODO (terence): Right now we can only evict if we see all the data columns.
      // It's because it's possible that in some previous flush, only the data columns are flush,
      // then consumer writes the state columns. In the next flush, it'll only see the state columns and those
      // should not be evicted otherwise the entry might get reprocessed, depending on the consumer start row state.
      // This logic is not perfect as if flush happens after enqueue and before dequeue, that entry may never get
      // evicted (depends on when the next compaction happens, whether the queue configuration has been change or not).

      // There are two data columns, "d" and "m".
      // If the size == 2, it should not be evicted as well,
      // as state columns (dequeue) always happen after data columns (enqueue).
      if (result.size() <= 2) {
        skippedIncomplete++;
        return false;
      }

      // "d" and "m" columns always comes before the state columns, prefixed with "s".
      Iterator<Cell> iterator = result.iterator();
      Cell cell = iterator.next();
      if (!QueueEntryRow.isDataColumn(cell.getQualifierArray(), cell.getQualifierOffset())) {
        skippedIncomplete++;
        return false;
      }
      cell = iterator.next();
      if (!QueueEntryRow.isMetaColumn(cell.getQualifierArray(), cell.getQualifierOffset())) {
        skippedIncomplete++;
        return false;
      }

      // Need to determine if this row can be evicted iff all consumer groups have committed process this row.
      int consumedGroups = 0;
      // Inspect each state column
      while (iterator.hasNext()) {
        cell = iterator.next();
        if (!QueueEntryRow.isStateColumn(cell.getQualifierArray(), cell.getQualifierOffset())) {
          continue;
        }
        // If any consumer has a state != PROCESSED, it should not be evicted
        if (!isProcessed(cell, consumerInstance)) {
          break;
        }
        // If it is PROCESSED, check if this row is smaller than the consumer instance startRow.
        // Essentially a loose check of committed PROCESSED.
        byte[] startRow = consumerConfig.getStartRow(consumerInstance);
        if (startRow != null && compareRowKey(cell, startRow) < 0) {
          consumedGroups++;
        }
      }

      // It can be evicted if from the state columns, it's been processed by all consumer groups
      // Otherwise, this row has to be less than smallest among all current consumers.
      // The second condition is for handling consumer being removed after it consumed some entries.
      // However, the second condition alone is not good enough as it's possible that in hash partitioning,
      // only one consumer is keep consuming when the other consumer never proceed.
      return consumedGroups == consumerConfig.getNumGroups()
        || compareRowKey(result.get(0), consumerConfig.getSmallestStartRow()) < 0;
    }

    private int compareRowKey(Cell cell, byte[] row) {
      return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset() + prefixBytes,
                             cell.getRowLength() - prefixBytes, row, 0, row.length);
    }

    /**
     * Returns {@code true} if the given {@link KeyValue} has a {@link ConsumerEntryState#PROCESSED} state and
     * also put the consumer information into the given {@link ConsumerInstance}.
     * Otherwise, returns {@code false} and the {@link ConsumerInstance} is left untouched.
     */
    private boolean isProcessed(Cell cell, ConsumerInstance consumerInstance) {
      int stateIdx = cell.getValueOffset() + cell.getValueLength() - 1;
      boolean processed = cell.getValueArray()[stateIdx] == ConsumerEntryState.PROCESSED.getState();

      if (processed) {
        // Column is "s<groupId>"
        long groupId = Bytes.toLong(cell.getQualifierArray(), cell.getQualifierOffset() + 1);
        // Value is "<writePointer><instanceId><state>"
        int instanceId = Bytes.toInt(cell.getValueArray(), cell.getValueOffset() + Bytes.SIZEOF_LONG);
        consumerInstance.setGroupInstance(groupId, instanceId);
      }
      return processed;
    }
  }
}
