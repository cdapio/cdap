package com.continuuity.data2.increment.hbase96;

import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import com.continuuity.data2.transaction.coprocessor.ReactorTransactionStateCacheSupplier;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCache;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCacheSupplier;
import com.continuuity.data2.transaction.coprocessor.hbase96.Filters;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 *
 */
public class IncrementHandler extends BaseRegionObserver {
  // prefix bytes used to mark values that are deltas vs. full sums
  public static final byte[] DELTA_MAGIC_PREFIX = new byte[] { 'X', 'D' };
  // expected length for values storing deltas (prefix + increment value)
  public static final int DELTA_FULL_LENGTH = DELTA_MAGIC_PREFIX.length + Bytes.SIZEOF_LONG;
  public static final int BATCH_UNLIMITED = -1;

  private static final Log LOG = LogFactory.getLog(IncrementHandler.class);

  private HRegion region;
  private TransactionStateCache cache;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      this.region = ((RegionCoprocessorEnvironment) e).getRegion();
      Supplier<TransactionStateCache> cacheSupplier = getTransactionStateCacheSupplier(env);
      this.cache = cacheSupplier.get();
    }
  }

  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    String tableName = env.getRegion().getTableDesc().getNameAsString();
    String[] parts = tableName.split("\\.", 2);
    String tableNamespace = "";
    if (parts.length > 0) {
      tableNamespace = parts[0];
    }
    return new ReactorTransactionStateCacheSupplier(tableNamespace, env.getConfiguration());
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> ctx, Get get, List<Cell> results)
    throws IOException {
    Scan scan = new Scan(get);
    scan.setMaxVersions();
    scan.setFilter(Filters.combine(new IncrementFilter(), scan.getFilter()));
    RegionScanner scanner = null;
    try {
      scanner = new IncrementSummingScanner(region, scan.getBatch(), region.getScanner(scan));
      scanner.next(results);
      ctx.bypass();
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
    throws IOException {
    if (put.getAttribute(HBaseOcTableClient.DELTA_WRITE) != null) {
      // incremental write
      NavigableMap<byte[], List<Cell>> newFamilyMap = new TreeMap<byte[], List<Cell>>(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        List<Cell> newCells = new ArrayList<Cell>(entry.getValue().size());
        for (Cell cell : entry.getValue()) {
          byte[] newValue = Bytes.add(DELTA_MAGIC_PREFIX, CellUtil.cloneValue(cell));
          newCells.add(CellUtil.createCell(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell),
                                           CellUtil.cloneQualifier(cell), cell.getTimestamp(), cell.getTypeByte(),
                                           newValue));
        }
        newFamilyMap.put(entry.getKey(), newCells);
      }
      put.setFamilyCellMap(newFamilyMap);
    }
    // put completes normally with overridden column qualifiers
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    // must see all versions to aggregate increments
    scan.setMaxVersions();
    scan.setFilter(Filters.combine(new IncrementFilter(), scan.getFilter()));
    return s;
  }

  @Override
  public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan,
                                       RegionScanner scanner)
    throws IOException {
    return new IncrementSummingScanner(region, scan.getBatch(), scanner);
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                  InternalScanner scanner) throws IOException {
    TransactionSnapshot snapshot = cache.getLatestState();
    if (snapshot != null) {
      return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner, snapshot.getVisibilityUpperBound());
    }
    return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner);
  }

  public static boolean isIncrement(Cell cell) {
    return cell.getValueLength() == IncrementHandler.DELTA_FULL_LENGTH &&
      Bytes.equals(cell.getValueArray(), cell.getValueOffset(), IncrementHandler.DELTA_MAGIC_PREFIX.length,
                   IncrementHandler.DELTA_MAGIC_PREFIX, 0, IncrementHandler.DELTA_MAGIC_PREFIX.length);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                    InternalScanner scanner, ScanType scanType) throws IOException {
    TransactionSnapshot snapshot = cache.getLatestState();
    if (snapshot != null) {
      return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner, snapshot.getVisibilityUpperBound());
    }
    return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                    InternalScanner scanner, ScanType scanType, CompactionRequest request)
    throws IOException {
    TransactionSnapshot snapshot = cache.getLatestState();
    if (snapshot != null) {
      return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner, snapshot.getVisibilityUpperBound());
    }
    return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner);
  }

  private static class IncrementFilter extends FilterBase {
    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      if (isIncrement(cell)) {
        // all visible increments should be included until we get to a non-increment
        return ReturnCode.INCLUDE;
      } else {
        // as soon as we find a KV to include we can move to the next column
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
    }
  }
}
