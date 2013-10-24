package com.continuuity.data2.transaction.coprocessor;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
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
      String tableName = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc().getNameAsString();
      String prefix = null;
      String[] parts = tableName.split("\\.", 2);
      if (parts.length > 0) {
        prefix = parts[0];
      }
      this.cache = TransactionStateCache.get(e.getConfiguration(), prefix);
    }
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
      return new DataJanitorRegionScanner(snapshot.getInvalid(), scanner,
                                          e.getEnvironment().getRegion().getRegionName());
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
      return new DataJanitorRegionScanner(cache.getLatestState().getInvalid(), scanner,
                                          e.getEnvironment().getRegion().getRegionName());
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
      return new DataJanitorRegionScanner(cache.getLatestState().getInvalid(), scanner,
                                          e.getEnvironment().getRegion().getRegionName());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Region " + e.getEnvironment().getRegion().getRegionNameAsString() +
                  ", no current transaction state found, defaulting to normal compaction scanner");
    }
    return scanner;
  }

  /**
   * Wraps the {@link org.apache.hadoop.hbase.regionserver.InternalScanner} instance used during compaction
   * to filter out any {@link org.apache.hadoop.hbase.KeyValue} entries associated with invalid transactions.
   */
  static class DataJanitorRegionScanner implements InternalScanner {
    private final Set<Long> invalidIds;
    private final InternalScanner internalScanner;
    private final List<KeyValue> internalResults = new ArrayList<KeyValue>();
    private final byte[] regionName;
    private long filteredCount = 0L;

    public DataJanitorRegionScanner(Collection<Long> invalidSet, InternalScanner scanner, byte[] regionName) {
      this.invalidIds = Sets.newHashSet(invalidSet);
      this.internalScanner = scanner;
      this.regionName = regionName;
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
      for (int i = 0; i < internalResults.size(); i++) {
        KeyValue kv = internalResults.get(i);
        // filter out any KeyValue with a timestamp matching an invalid write pointer
        if (!invalidIds.contains(kv.getTimestamp())) {
          results.add(kv);
        } else {
          filteredCount++;
        }
      }

      return hasMore;
    }

    @Override
    public void close() throws IOException {
      LOG.info("Region " + Bytes.toStringBinary(regionName) + " filtered out " + filteredCount + " KeyValues");
      this.internalScanner.close();
    }
  }
}
