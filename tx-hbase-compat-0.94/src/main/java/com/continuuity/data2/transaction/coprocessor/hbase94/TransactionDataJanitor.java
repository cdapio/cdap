/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.coprocessor.hbase94;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionCodec;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCache;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCacheSupplier;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.util.TxUtils;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 *   <value>com.continuuity.data2.transaction.coprocessor.hbase94.TransactionDataJanitor</value>
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
public class TransactionDataJanitor extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(TransactionDataJanitor.class);

  private TransactionStateCache cache;
  private final TransactionCodec txCodec;
  private Map<byte[], Long> ttlByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  private boolean allowEmptyValues = TxConstants.ALLOW_EMPTY_VALUES_DEFAULT;

  public TransactionDataJanitor() {
    this.txCodec = new TransactionCodec();
  }

  /* RegionObserver implementation */

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      Supplier<TransactionStateCache> cacheSupplier = getTransactionStateCacheSupplier(env);
      this.cache = cacheSupplier.get();

      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      for (HColumnDescriptor columnDesc : tableDesc.getFamilies()) {
        String columnTTL = columnDesc.getValue(TxConstants.PROPERTY_TTL);
        long ttl = 0;
        if (columnTTL != null) {
          try {
            ttl = Long.parseLong(columnTTL);
          } catch (NumberFormatException nfe) {
            LOG.warn("Invalid TTL value configured for column family " + columnDesc.getNameAsString() +
                       ", value = " + columnTTL);
          }
        }
        ttlByFamily.put(columnDesc.getName(), ttl);
      }

      this.allowEmptyValues = env.getConfiguration().getBoolean(TxConstants.ALLOW_EMPTY_VALUES_KEY,
                                                                TxConstants.ALLOW_EMPTY_VALUES_DEFAULT);
    }
  }

  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    return new TransactionStateCacheSupplier(env.getConfiguration());
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    // nothing to do
  }

  @Override
  public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results)
    throws IOException {
    Transaction tx = txCodec.getFromOperation(get);
    if (tx != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Applying filter to GET for transaction " + tx.getWritePointer());
      }
      get.setMaxVersions(tx.excludesSize() + 1);
      get.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx), TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = combineFilters(new TransactionVisibilityFilter(tx, ttlByFamily, allowEmptyValues),
                                        get.getFilter());
      get.setFilter(newFilter);
    }
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    Transaction tx = txCodec.getFromOperation(scan);
    if (tx != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Applying filter to SCAN for transaction " + tx.getWritePointer());
      }
      scan.setMaxVersions(tx.excludesSize() + 1);
      scan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx), TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = combineFilters(new TransactionVisibilityFilter(tx, ttlByFamily, allowEmptyValues),
                                        scan.getFilter());
      scan.setFilter(newFilter);
    }
    return s;
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
      InternalScanner scanner) throws IOException {
    TransactionSnapshot snapshot = cache.getLatestState();
    if (snapshot != null) {
      return createDataJanitorRegionScanner(e, store, scanner, snapshot);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Region " + e.getEnvironment().getRegion().getRegionNameAsString() +
                  ", no current transaction state found, defaulting to normal flush scanner");
    }
    return scanner;
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
      InternalScanner scanner) throws IOException {
    TransactionSnapshot snapshot = cache.getLatestState();
    if (snapshot != null) {
      return createDataJanitorRegionScanner(e, store, scanner, snapshot);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Region " + e.getEnvironment().getRegion().getRegionNameAsString() +
                  ", no current transaction state found, defaulting to normal compaction scanner");
    }
    return scanner;
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
      InternalScanner scanner, CompactionRequest request) throws IOException {
    TransactionSnapshot snapshot = cache.getLatestState();
    if (snapshot != null) {
      return createDataJanitorRegionScanner(e, store, scanner, snapshot);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Region " + e.getEnvironment().getRegion().getRegionNameAsString() +
                  ", no current transaction state found, defaulting to normal compaction scanner");
    }
    return scanner;
  }

  private Filter combineFilters(Filter overrideFilter, Filter baseFilter) {
    if (baseFilter != null) {
      FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filterList.addFilter(baseFilter);
      filterList.addFilter(overrideFilter);
      return filterList;
    }
    return overrideFilter;
  }

  private DataJanitorRegionScanner createDataJanitorRegionScanner(ObserverContext<RegionCoprocessorEnvironment> e,
                                                                  Store store,
                                                                  InternalScanner scanner,
                                                                  TransactionSnapshot snapshot) {
    long visibilityUpperBound = snapshot.getVisibilityUpperBound();
    String ttlProp = store.getFamily().getValue(TxConstants.PROPERTY_TTL);
    int ttl = ttlProp == null ? -1 : Integer.valueOf(ttlProp);
    // NOTE: to make sure we do not cleanup smth visible to tx in between its reads,
    // we use visibilityUpperBound as current ts
    long oldestToKeep = ttl <= 0 ? -1 : visibilityUpperBound - ttl * TxConstants.MAX_TX_PER_MS;

    return new DataJanitorRegionScanner(visibilityUpperBound, oldestToKeep,
                                        snapshot.getInvalid(), scanner,
                                        e.getEnvironment().getRegion().getRegionName());
  }

  /**
   * Wraps the {@link org.apache.hadoop.hbase.regionserver.InternalScanner} instance used during compaction
   * to filter out any {@link org.apache.hadoop.hbase.KeyValue} entries associated with invalid transactions.
   */
  static class DataJanitorRegionScanner implements InternalScanner {
    private final long visibilityUpperBound;
    // oldest tx to keep based on ttl
    private final long oldestToKeep;
    private final Set<Long> invalidIds;
    private final InternalScanner internalScanner;
    private final List<KeyValue> internalResults = new ArrayList<KeyValue>();
    private final byte[] regionName;
    private long expiredFilteredCount = 0L;
    private long invalidFilteredCount = 0L;
    // old and redundant: no tx will ever read them
    private long oldFilteredCount = 0L;

    public DataJanitorRegionScanner(long visibilityUpperBound, long oldestToKeep, Collection<Long> invalidSet,
                                    InternalScanner scanner, byte[] regionName) {
      this.visibilityUpperBound = visibilityUpperBound;
      this.oldestToKeep = oldestToKeep;
      this.invalidIds = Sets.newHashSet(invalidSet);
      this.internalScanner = scanner;
      this.regionName = regionName;
      LOG.info("Created new scanner with visibilityUpperBound: " + visibilityUpperBound +
                 ", invalid set: " + invalidIds + ", oldestToKeep: " + oldestToKeep);
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
      return next(results, -1, null);
    }

    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
      return next(results, -1, metric);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit) throws IOException {
      return next(results, limit, null);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit, String metric) throws IOException {
      internalResults.clear();

      boolean hasMore = internalScanner.next(internalResults, limit, metric);
      // TODO: due to filtering our own results may be smaller than limit, so we should retry if needed to hit it

      KeyValue previousKv = null;
      // tells to skip those equal to current cell in case when we met one that is not newer than the oldest of
      // currently used readPointers
      boolean skipSameCells = false;

      for (KeyValue kv : internalResults) {
        // filter out by ttl
        if (kv.getTimestamp() < oldestToKeep) {
          expiredFilteredCount++;
          continue;
        }

        // filter out any KeyValue with a timestamp matching an invalid write pointer
        if (invalidIds.contains(kv.getTimestamp())) {
          invalidFilteredCount++;
          continue;
        }

        boolean sameAsPreviousCell = previousKv != null && sameCell(kv, previousKv);
        // TODO: should check if this is a delete (empty byte[]) and !allowEmptyValues, then drop and skip to next col
        // skip same as previous if told so
        if (sameAsPreviousCell && skipSameCells) {
          oldFilteredCount++;
          continue;
        }

        // at this point we know we want to include it
        results.add(kv);

        if (!sameAsPreviousCell) {
          // this cell is different from previous, resetting state
          previousKv = kv;
        }

        // we met at least one version that is not newer than the oldest of currently used readPointers hence we
        // can skip older ones
        skipSameCells = kv.getTimestamp() <= visibilityUpperBound;
      }

      return hasMore;
    }

    private boolean sameCell(KeyValue first, KeyValue second) {
      return first.matchingRow(second) &&
        first.matchingFamily(second) &&
        first.matchingQualifier(second);
    }

    @Override
    public void close() throws IOException {
      LOG.info("Region " + Bytes.toStringBinary(regionName) +
                 " filtered out invalid/old/expired "
                 + invalidFilteredCount + "/" + oldFilteredCount + "/" + expiredFilteredCount + " KeyValues");
      this.internalScanner.close();
    }
  }
}
