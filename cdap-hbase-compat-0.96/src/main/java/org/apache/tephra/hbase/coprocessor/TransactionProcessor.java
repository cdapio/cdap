/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.hbase.coprocessor;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.CacheSupplier;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.coprocessor.TransactionStateCacheSupplier;
import org.apache.tephra.hbase.txprune.CompactionState;
import org.apache.tephra.persist.TransactionVisibilityState;
import org.apache.tephra.util.TxUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@code org.apache.hadoop.hbase.coprocessor.RegionObserver} coprocessor that handles server-side processing
 * for transactions:
 * <ul>
 *   <li>applies filtering to exclude data from invalid and in-progress transactions</li>
 *   <li>overrides the scanner returned for flush and compaction to drop data written by invalidated transactions,
 *   or expired due to TTL.</li>
 * </ul>
 *
 * <p>In order to use this coprocessor for transactions, configure the class on any table involved in transactions,
 * or on all user tables by adding the following to hbase-site.xml:
 * {@code
 * <property>
 *   <name>hbase.coprocessor.region.classes</name>
 *   <value>org.apache.tephra.hbase.coprocessor.TransactionProcessor</value>
 * </property>
 * }
 * </p>
 *
 * <p>HBase {@code Get} and {@code Scan} operations should have the current transaction serialized on to the operation
 * as an attribute:
 * {@code
 * Transaction t = ...;
 * Get get = new Get(...);
 * TransactionCodec codec = new TransactionCodec();
 * codec.addToOperation(get, t);
 * }
 * </p>
 */
public class TransactionProcessor extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(TransactionProcessor.class);

  private final TransactionCodec txCodec;
  private TransactionStateCache cache;
  private volatile CompactionState compactionState;
  private CacheSupplier<TransactionStateCache> cacheSupplier;

  protected volatile Boolean pruneEnable;
  protected volatile Long txMaxLifetimeMillis;
  protected Map<byte[], Long> ttlByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  protected boolean allowEmptyValues = TxConstants.ALLOW_EMPTY_VALUES_DEFAULT;
  protected boolean readNonTxnData = TxConstants.DEFAULT_READ_NON_TX_DATA;

  public TransactionProcessor() {
    this.txCodec = new TransactionCodec();
  }

  /* RegionObserver implementation */

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      this.cacheSupplier = getTransactionStateCacheSupplier(env);
      this.cache = cacheSupplier.get();

      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      for (HColumnDescriptor columnDesc : tableDesc.getFamilies()) {
        String columnTTL = columnDesc.getValue(TxConstants.PROPERTY_TTL);
        long ttl = 0;
        if (columnTTL != null) {
          try {
            ttl = Long.parseLong(columnTTL);
            LOG.info("Family " + columnDesc.getNameAsString() + " has TTL of " + columnTTL);
          } catch (NumberFormatException nfe) {
            LOG.warn("Invalid TTL value configured for column family " + columnDesc.getNameAsString() +
                       ", value = " + columnTTL);
          }
        }
        ttlByFamily.put(columnDesc.getName(), ttl);
      }

      this.allowEmptyValues = getAllowEmptyValues(env, tableDesc);
      this.txMaxLifetimeMillis = getTxMaxLifetimeMillis(env);
      this.readNonTxnData = Boolean.valueOf(tableDesc.getValue(TxConstants.READ_NON_TX_DATA));
      if (readNonTxnData) {
        LOG.info("Reading pre-existing data enabled for table " + tableDesc.getNameAsString());
      }
      initializePruneState(env);
    }
  }

  /**
   * Fetch the {@link Configuration} that contains the properties required by the coprocessor. By default,
   * the HBase configuration is returned. This method will never return {@code null} in Tephra but the derived
   * classes might do so if {@link Configuration} is not available temporarily (for example, if it is being fetched
   * from a HBase Table.
   *
   * @param env {@link RegionCoprocessorEnvironment} of the Region to which the coprocessor is associated
   * @return {@link Configuration}, can be null if it is not available
   */
  @Nullable
  protected Configuration getConfiguration(CoprocessorEnvironment env) {
    return env.getConfiguration();
  }

  protected CacheSupplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    return new TransactionStateCacheSupplier(env.getConfiguration());
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    try {
      resetPruneState();
    } finally {
      if (cacheSupplier != null) {
        cacheSupplier.release();
      }
    }
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
    throws IOException {
    Transaction tx = getFromOperation(get);
    if (tx != null) {
      projectFamilyDeletes(get);
      get.setMaxVersions();
      get.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx, readNonTxnData),
                       TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = getTransactionFilter(tx, ScanType.USER_SCAN, get.getFilter());
      get.setFilter(newFilter);
    }
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
    throws IOException {
    Transaction tx = getFromOperation(put);
    ensureValidTxLifetime(e.getEnvironment(), put, tx);
  }

  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                        Durability durability) throws IOException {
    // Translate deletes into our own delete tombstones
    // Since HBase deletes cannot be undone, we need to translate deletes into special puts, which allows
    // us to rollback the changes (by a real delete) if the transaction fails

    // Deletes that are part of a transaction rollback do not need special handling.
    // They will never be rolled back, so are performed as normal HBase deletes.
    if (isRollbackOperation(delete)) {
      return;
    }

    Transaction tx = getFromOperation(delete);
    ensureValidTxLifetime(e.getEnvironment(), delete, tx);

    // Other deletes are client-initiated and need to be translated into our own tombstones
    // TODO: this should delegate to the DeleteStrategy implementation.
    Put deleteMarkers = new Put(delete.getRow(), delete.getTimeStamp());
    for (byte[] family : delete.getFamilyCellMap().keySet()) {
      List<Cell> familyCells = delete.getFamilyCellMap().get(family);
      if (isFamilyDelete(familyCells)) {
        deleteMarkers.add(family, TxConstants.FAMILY_DELETE_QUALIFIER, familyCells.get(0).getTimestamp(),
                          HConstants.EMPTY_BYTE_ARRAY);
      } else {
        for (Cell cell : familyCells) {
          deleteMarkers.add(family, CellUtil.cloneQualifier(cell), cell.getTimestamp(),
                            HConstants.EMPTY_BYTE_ARRAY);
        }
      }
    }
    for (Map.Entry<String, byte[]> entry : delete.getAttributesMap().entrySet()) {
        deleteMarkers.setAttribute(entry.getKey(), entry.getValue());
    }
    e.getEnvironment().getRegion().put(deleteMarkers);
    // skip normal delete handling
    e.bypass();
  }

  private boolean getAllowEmptyValues(RegionCoprocessorEnvironment env, HTableDescriptor htd) {
    String allowEmptyValuesFromTableDesc = htd.getValue(TxConstants.ALLOW_EMPTY_VALUES_KEY);
    Configuration conf = getConfiguration(env);
    boolean allowEmptyValuesFromConfig = (conf != null) ?
      conf.getBoolean(TxConstants.ALLOW_EMPTY_VALUES_KEY, TxConstants.ALLOW_EMPTY_VALUES_DEFAULT) :
      TxConstants.ALLOW_EMPTY_VALUES_DEFAULT;

    // If the property is not present in the tableDescriptor, get it from the Configuration
    return  (allowEmptyValuesFromTableDesc != null) ?
      Boolean.valueOf(allowEmptyValuesFromTableDesc) : allowEmptyValuesFromConfig;
  }

  private long getTxMaxLifetimeMillis(RegionCoprocessorEnvironment env) {
    Configuration conf = getConfiguration(env);
    if (conf != null) {
      return TimeUnit.SECONDS.toMillis(conf.getInt(TxConstants.Manager.CFG_TX_MAX_LIFETIME,
                                                   TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME));
    }
    return TimeUnit.SECONDS.toMillis(TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME);
  }

  private boolean isFamilyDelete(List<Cell> familyCells) {
    return familyCells.size() == 1 && CellUtil.isDeleteFamily(familyCells.get(0));
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    Transaction tx = getFromOperation(scan);
    if (tx != null) {
      projectFamilyDeletes(scan);
      scan.setMaxVersions();
      scan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx, readNonTxnData),
                        TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = getTransactionFilter(tx, ScanType.USER_SCAN, scan.getFilter());
      scan.setFilter(newFilter);
    }
    return s;
  }

  /**
   * Ensures that family delete markers are present in the columns requested for any scan operation.
   * @param scan The original scan request
   * @return The modified scan request with the family delete qualifiers represented
   */
  private Scan projectFamilyDeletes(Scan scan) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
      NavigableSet<byte[]> columns = entry.getValue();
      // wildcard scans will automatically include the delete marker, so only need to add it when we have
      // explicit columns listed
      if (columns != null && !columns.isEmpty()) {
        scan.addColumn(entry.getKey(), TxConstants.FAMILY_DELETE_QUALIFIER);
      }
    }
    return scan;
  }

  /**
   * Ensures that family delete markers are present in the columns requested for any get operation.
   * @param get The original get request
   * @return The modified get request with the family delete qualifiers represented
   */
  private Get projectFamilyDeletes(Get get) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap().entrySet()) {
      NavigableSet<byte[]> columns = entry.getValue();
      // wildcard scans will automatically include the delete marker, so only need to add it when we have
      // explicit columns listed
      if (columns != null && !columns.isEmpty()) {
        get.addColumn(entry.getKey(), TxConstants.FAMILY_DELETE_QUALIFIER);
      }
    }
    return get;
  }

  @Override
  public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                             KeyValueScanner memstoreScanner, InternalScanner scanner)
      throws IOException {
    return createStoreScanner(c.getEnvironment(), "flush", cache.getLatestState(), store,
                              Collections.singletonList(memstoreScanner), ScanType.COMPACT_RETAIN_DELETES,
                              HConstants.OLDEST_TIMESTAMP);
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
      List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s,
      CompactionRequest request)
      throws IOException {
    // Get the latest tx snapshot state for the compaction
    TransactionVisibilityState snapshot = cache.getLatestState();

    // Record tx state before the compaction
    if (compactionState != null) {
      compactionState.record(request, snapshot);
    }

    // Also make sure to use the same snapshot for the compaction
    return createStoreScanner(c.getEnvironment(), "compaction", snapshot, store, scanners, scanType, earliestPutTs);
  }

  @Override
  public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile,
                          CompactionRequest request) throws IOException {
    // Persist the compaction state after a succesful compaction
    if (compactionState != null) {
      compactionState.persist();
    }
  }

  protected InternalScanner createStoreScanner(RegionCoprocessorEnvironment env, String action,
                                               TransactionVisibilityState snapshot, Store store,
                                               List<? extends KeyValueScanner> scanners, ScanType type,
                                               long earliestPutTs) throws IOException {
    if (snapshot == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region " + env.getRegion().getRegionNameAsString() +
                    ", no current transaction state found, defaulting to normal " + action + " scanner");
      }
      return null;
    }

    // construct a dummy transaction from the latest snapshot
    Transaction dummyTx = TxUtils.createDummyTransaction(snapshot);
    Scan scan = new Scan();
    // need to see all versions, since we filter out excludes and applications may rely on multiple versions
    scan.setMaxVersions();
    scan.setFilter(
        new IncludeInProgressFilter(dummyTx.getVisibilityUpperBound(),
            snapshot.getInvalid(),
            getTransactionFilter(dummyTx, type, null)));

    return new StoreScanner(store, store.getScanInfo(), scan, scanners,
                            type, store.getSmallestReadPoint(), earliestPutTs);
  }

  private Transaction getFromOperation(OperationWithAttributes op) throws IOException {
    byte[] encoded = op.getAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY);
    if (encoded == null) {
      // to support old clients
      encoded = op.getAttribute(TxConstants.OLD_TX_OPERATION_ATTRIBUTE_KEY);
    }
    if (encoded != null) {
      return txCodec.decode(encoded);
    }
    return null;
  }

  /**
   * Make sure that the transaction is within the max valid transaction lifetime.
   *
   * @param env {@link RegionCoprocessorEnvironment} of the Region to which the coprocessor is associated
   * @param op {@link OperationWithAttributes} HBase operation to access its attributes if required
   * @param tx {@link Transaction} supplied by the
   * @throws DoNotRetryIOException thrown if the transaction is older than the max lifetime of a transaction
   *         IOException throw if the value of max lifetime of transaction is unavailable
   */
  protected void ensureValidTxLifetime(RegionCoprocessorEnvironment env,
                                       @SuppressWarnings("unused") OperationWithAttributes op,
                                       @Nullable Transaction tx) throws IOException {
    if (tx == null) {
      return;
    }

    boolean validLifetime =
      (TxUtils.getTimestamp(tx.getTransactionId()) + txMaxLifetimeMillis) > System.currentTimeMillis();
    if (!validLifetime) {
      throw new DoNotRetryIOException(String.format("Transaction %s has exceeded max lifetime %s ms",
                                                    tx.getTransactionId(), txMaxLifetimeMillis));
    }
  }

  private boolean isRollbackOperation(OperationWithAttributes op) throws IOException {
    return op.getAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY) != null ||
      // to support old clients
      op.getAttribute(TxConstants.OLD_TX_ROLLBACK_ATTRIBUTE_KEY) != null;
  }

  /**
   * Derived classes can override this method to customize the filter used to return data visible for the current
   * transaction.
   *
   * @param tx the current transaction to apply
   * @param type the type of scan operation being performed
   */
  protected Filter getTransactionFilter(Transaction tx, ScanType type, Filter filter) {
    return TransactionFilters.getVisibilityFilter(tx, ttlByFamily, allowEmptyValues, readNonTxnData, type, filter);
  }

  /**
   * Refresh the properties related to transaction pruning. This method needs to be invoked if there is change in the
   * prune related properties after clearing the state by calling {@link #resetPruneState}.
   *
   * @param env {@link RegionCoprocessorEnvironment} of this region
   */
  protected void initializePruneState(RegionCoprocessorEnvironment env) {
    Configuration conf = getConfiguration(env);
    if (conf != null) {
      pruneEnable = conf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE,
                                    TxConstants.TransactionPruning.DEFAULT_PRUNE_ENABLE);

      if (Boolean.TRUE.equals(pruneEnable)) {
        TableName pruneTable = TableName.valueOf(conf.get(TxConstants.TransactionPruning.PRUNE_STATE_TABLE,
                                                          TxConstants.TransactionPruning.DEFAULT_PRUNE_STATE_TABLE));
        long pruneFlushInterval = TimeUnit.SECONDS.toMillis(conf.getLong(
          TxConstants.TransactionPruning.PRUNE_FLUSH_INTERVAL,
          TxConstants.TransactionPruning.DEFAULT_PRUNE_FLUSH_INTERVAL));

        compactionState = new CompactionState(env, pruneTable, pruneFlushInterval);
        if (LOG.isDebugEnabled()) {
          TableName name = env.getRegion().getRegionInfo().getTable();
          LOG.debug(String.format("Automatic invalid list pruning is enabled for table %s:%s. Compaction state will " +
                                    "be recorded in table %s:%s", name.getNamespaceAsString(), name.getNameAsString(),
                                  pruneTable.getNamespaceAsString(), pruneTable.getNameAsString()));
        }
      }
    }
  }

  /**
   * Stop and clear state related to pruning.
   */
  protected void resetPruneState() {
    pruneEnable = false;
    if (compactionState != null) {
      compactionState.stop();
      compactionState = null;
    }
  }

  private long numStoreFilesForRegion(ObserverContext<RegionCoprocessorEnvironment> c) {
    long numStoreFiles = 0;
    for (Store store : c.getEnvironment().getRegion().getStores().values()) {
      numStoreFiles += store.getStorefiles().size();
    }
    return numStoreFiles;
  }

  /**
   * Filter used to include cells visible to in-progress transactions on flush and commit.
   */
  static class IncludeInProgressFilter extends FilterBase {
    private final long visibilityUpperBound;
    private final Set<Long> invalidIds;
    private final Filter txFilter;

    public IncludeInProgressFilter(long upperBound, Collection<Long> invalids, Filter transactionFilter) {
      this.visibilityUpperBound = upperBound;
      this.invalidIds = Sets.newHashSet(invalids);
      this.txFilter = transactionFilter;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      // include all cells visible to in-progress transactions, except for those already marked as invalid
      long ts = cell.getTimestamp();
      if (ts > visibilityUpperBound) {
        // include everything that could still be in-progress except invalids
        if (invalidIds.contains(ts)) {
          return ReturnCode.SKIP;
        }
        return ReturnCode.INCLUDE;
      }
      return txFilter.filterKeyValue(cell);
    }
  }
}
