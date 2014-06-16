package com.continuuity.data2.transaction.coprocessor.hbase94;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCache;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCacheSupplier;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver} coprocessor that removes data from invalid transactions
 * during region compactions.
 */
public class TransactionDataJanitor extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(TransactionDataJanitor.class);

  private TransactionStateCache cache;

  /* RegionObserver implementation */

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      Supplier<TransactionStateCache> cacheSupplier =
        getTransactionStateCacheSupplier((RegionCoprocessorEnvironment) e);
      this.cache = cacheSupplier.get();
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
