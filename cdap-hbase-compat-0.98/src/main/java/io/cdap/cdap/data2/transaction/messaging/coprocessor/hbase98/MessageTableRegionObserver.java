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

package co.cask.cdap.data2.transaction.messaging.coprocessor.hbase98;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.cdap.data2.util.hbase.CConfigurationReader;
import co.cask.cdap.data2.util.hbase.CoprocessorCConfigurationReader;
import co.cask.cdap.data2.util.hbase.DefaultScanBuilder;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicMetadataCache;
import co.cask.cdap.messaging.TopicMetadataCacheSupplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.coprocessor.TransactionStateCacheSupplier;
import org.apache.tephra.hbase.txprune.CompactionState;
import org.apache.tephra.persist.TransactionVisibilityState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * RegionObserver for the Message Table of Transactional Messaging Service.
 *
 * This region observer does row eviction during flush time and compact time by using
 *
 * i) Time-To-Live (TTL) information in MetadataTable to determine if a message has expired its TTL.
 * ii) Generation information in MetadataTable to determine if the message belongs to older generation.
 * iii) Mark data entries with invalidated transactions (loaded from tx.snapshot) so that invalid list can be pruned.
 *
 */
public class MessageTableRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(MessageTableRegionObserver.class);

  private int prefixLength;
  private TransactionStateCacheSupplier txStateCacheSupplier;
  private TransactionStateCache txStateCache;
  private TopicMetadataCache topicMetadataCache;

  private TopicMetadataCacheSupplier topicMetadataCacheSupplier;
  private CompactionState compactionState;
  private Boolean pruneEnable;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      String metadataTableNamespace = tableDesc.getValue(Constants.MessagingSystem.HBASE_METADATA_TABLE_NAMESPACE);
      prefixLength = Integer.valueOf(tableDesc.getValue(
        Constants.MessagingSystem.HBASE_MESSAGING_TABLE_PREFIX_NUM_BYTES));

      String tablePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);
      CConfigurationReader cConfReader = new CoprocessorCConfigurationReader(env, tablePrefix);
      txStateCacheSupplier = new DefaultTransactionStateCacheSupplier(tablePrefix, env);
      txStateCache = txStateCacheSupplier.get();
      topicMetadataCacheSupplier = new TopicMetadataCacheSupplier(
        env, cConfReader, tablePrefix, metadataTableNamespace, new DefaultScanBuilder());
      topicMetadataCache = topicMetadataCacheSupplier.get();
    }
  }

  @Override
  public void stop(CoprocessorEnvironment e) {
    if (compactionState != null) {
      compactionState.stop();
    }
    topicMetadataCacheSupplier.release();
    txStateCacheSupplier.release();
  }

  @Override
  public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                             KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
    LOG.info("preFlush, filter using MessageDataFilter");
    TransactionVisibilityState txVisibilityState = txStateCache.getLatestState();
    Scan scan = new Scan();
    scan.setFilter(new MessageDataFilter(c.getEnvironment(), System.currentTimeMillis(),
                                         prefixLength, topicMetadataCache, txVisibilityState));
    return new LoggingInternalScanner("MessageDataFilter", "preFlush",
                                      new StoreScanner(store, store.getScanInfo(), scan,
                                                       Collections.singletonList(memstoreScanner),
                                                       ScanType.COMPACT_DROP_DELETES, store.getSmallestReadPoint(),
                                                       HConstants.OLDEST_TIMESTAMP), txVisibilityState);
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

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                               List<? extends KeyValueScanner> scanners, ScanType scanType,
                                               long earliestPutTs, InternalScanner s,
                                               CompactionRequest request) throws IOException {
    LOG.info("preCompact, filter using MessageDataFilter");
    TransactionVisibilityState txVisibilityState = txStateCache.getLatestState();

    reloadPruneState(c.getEnvironment());
    if (compactionState != null) {
      // Record tx state before the compaction
      compactionState.record(request, txVisibilityState);
    }

    Scan scan = new Scan();
    scan.setFilter(new MessageDataFilter(c.getEnvironment(), System.currentTimeMillis(),
                                         prefixLength, topicMetadataCache, txVisibilityState));
    return new LoggingInternalScanner("MessageDataFilter", "preCompact",
                                      new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType,
                                                       store.getSmallestReadPoint(), earliestPutTs), txVisibilityState);
  }

  @Override
  public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile,
                          CompactionRequest request) throws IOException {
    // Persist the compaction state after a successful compaction
    if (compactionState != null) {
      compactionState.persist();
    }
  }

  private void reloadPruneState(RegionCoprocessorEnvironment env) {
    if (pruneEnable == null) {
      // If prune enable has never been initialized, try to do so now
      initializePruneState(env);
    } else {
      CConfiguration conf = topicMetadataCache.getCConfiguration();
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
    CConfiguration conf = topicMetadataCache.getCConfiguration();
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

  private static final class LoggingInternalScanner implements InternalScanner {
    private static final Log LOG = LogFactory.getLog(LoggingInternalScanner.class);

    private final String filterName;
    private final String operation;
    private final InternalScanner delegate;
    private final TransactionVisibilityState state;

    LoggingInternalScanner(String filterName, String operation, InternalScanner delegate,
                           @Nullable TransactionVisibilityState state) {
      this.filterName = filterName;
      this.operation = operation;
      this.delegate = delegate;
      this.state = state;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      return delegate.next(results);
    }

    @Override
    public boolean next(List<Cell> list, int limit) throws IOException {
      return delegate.next(list, limit);
    }

    @Override
    public void close() throws IOException {
      if (state != null) {
        LOG.info("During " + operation + ", Filter " + filterName + " completed using tx.snapshot of timestamp "
                   + state.getTimestamp() + " which had the visibility bound of " + state.getVisibilityUpperBound());
      }
      delegate.close();
    }
  }

  private long numStoreFilesForRegion(ObserverContext<RegionCoprocessorEnvironment> c) {
    long numStoreFiles = 0;
    for (Store store : c.getEnvironment().getRegion().getStores().values()) {
      numStoreFiles += store.getStorefiles().size();
    }
    return numStoreFiles;
  }

  private static final class MessageDataFilter extends FilterBase {
    private final RegionCoprocessorEnvironment env;
    private final long timestamp;
    private final TransactionVisibilityState state;
    private final int prefixLength;
    private final TopicMetadataCache metadataCache;

    private byte[] prevTopicIdBytes;
    private Long currentTTL;
    private Integer currentGen;

    MessageDataFilter(RegionCoprocessorEnvironment env, long timestamp, int prefixLength,
                      TopicMetadataCache metadataCache, @Nullable TransactionVisibilityState state) {
      this.env = env;
      this.timestamp = timestamp;
      this.prefixLength = prefixLength;
      this.metadataCache = metadataCache;
      this.state = state;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      int rowKeyOffset = cell.getRowOffset() + prefixLength;
      int sizeOfRowKey = cell.getRowLength() - prefixLength;
      long publishTimestamp = MessagingUtils.getPublishTimestamp(cell.getRowArray(), rowKeyOffset, sizeOfRowKey);
      int topicIdLength = MessagingUtils.getTopicLengthMessageEntry(sizeOfRowKey) - Bytes.SIZEOF_INT;
      int generationId = Bytes.toInt(cell.getRowArray(), rowKeyOffset + topicIdLength);

      if (prevTopicIdBytes == null || currentTTL == null || currentGen == null ||
        (!Bytes.equals(prevTopicIdBytes, 0, prevTopicIdBytes.length,
                       cell.getRowArray(), rowKeyOffset, topicIdLength))) {
        prevTopicIdBytes = Arrays.copyOfRange(cell.getRowArray(), rowKeyOffset, rowKeyOffset + topicIdLength);
        Map<String, String> properties = metadataCache.getTopicMetadata(ByteBuffer.wrap(prevTopicIdBytes));
        if (properties == null) {
          LOG.debug("Region " + env.getRegion().getRegionNameAsString() + ", could not get properties of topicId "
                      + MessagingUtils.toTopicId(prevTopicIdBytes));
          return ReturnCode.INCLUDE;
        }
        currentTTL = Long.parseLong(properties.get(MessagingUtils.Constants.TTL_KEY));
        currentGen = Integer.parseInt(properties.get(MessagingUtils.Constants.GENERATION_KEY));
      }

      // Old Generation (or deleted current generation) cleanup
      if (MessagingUtils.isOlderGeneration(generationId, currentGen)) {
        return ReturnCode.SKIP;
      }

      // TTL expiration cleanup only if the generation of metadata and row key are the same
      if ((generationId == currentGen) && ((timestamp - publishTimestamp) > currentTTL)) {
        return ReturnCode.SKIP;
      }
      return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell cell) throws IOException {
      // Rollback expired/invalidated transactions
      byte[] txCol = MessagingUtils.Constants.TX_COL;
      if (Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                       txCol, 0, txCol.length)) {
        // This is a TX COL
        long txWritePtr = Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
        if (txWritePtr > 0 && state != null && state.getInvalid().contains(txWritePtr)) {
          // tx is invalid and it hasn't been rolled back.
          byte[] backingArray = cell.getRowArray();
          Bytes.putLong(backingArray, cell.getValueOffset(), -1 * txWritePtr);
          return cell;
        }
      }
      return super.transformCell(cell);
    }
  }
}
