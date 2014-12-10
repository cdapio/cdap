/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.increment.hbase96;

import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTable;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.tephra.coprocessor.TransactionStateCache;
import co.cask.tephra.hbase96.Filters;
import co.cask.tephra.persist.TransactionSnapshot;
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
 * HBase coprocessor that handles reading and writing read-less increment operations.
 *
 * <p>Writes of incremental values are performed as normal {@code Put}s, flagged with a special attribute
 * {@link co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTable#DELTA_WRITE}.  The coprocessor intercepts these
 * writes and rewrites the cell value to use a special marker prefix.</p>
 *
 * <p>For read (for {@code Get} and {@code Scan}) operations, all of the delta values are summed up for a column,
 * up to and including the most recent "full" (non-delta) value.  The sum of these delta values, plus the full value
 * (if found) is returned for the column.</p>
 *
 * <p>To mitigate the performance impact on reading, this coprocessor also overrides the scanner used in flush and
 * compaction operations, using {@link IncrementSummingScanner} to generate a new "full" value aggregated from
 * all the successfully committed delta values.</p>
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
    return new DefaultTransactionStateCacheSupplier(tableNamespace, env.getConfiguration());
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
    if (put.getAttribute(HBaseOrderedTable.DELTA_WRITE) != null) {
      // incremental write
      NavigableMap<byte[], List<Cell>> newFamilyMap = new TreeMap<byte[], List<Cell>>(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        List<Cell> newCells = new ArrayList<Cell>(entry.getValue().size());
        for (Cell cell : entry.getValue()) {
          newCells.add(createDeltaIncrement(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell),
                                            CellUtil.cloneQualifier(cell), cell.getTimestamp(),
                                            cell.getTypeByte(), CellUtil.cloneValue(cell)));
        }
        newFamilyMap.put(entry.getKey(), newCells);
      }
      put.setFamilyCellMap(newFamilyMap);
    }
    // put completes normally with value prefix marker
  }

  static Cell createDeltaIncrement(final byte[] row, final byte[] family, final byte[] qualifier,
                                   final long timestamp, final byte type, final byte[] value) {
    // rewrite the cell value with a special prefix to identify it as a delta
    // for 0.98 we can update this to use cell tags
    byte[] deltaValue = Bytes.add(DELTA_MAGIC_PREFIX, value);
    return CellUtil.createCell(row, family, qualifier, timestamp, type, deltaValue);
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
    return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner,
                                       // non-user scan that cannot drop things
                                       ScanType.COMPACT_RETAIN_DELETES,
                                       snapshot != null ? snapshot.getVisibilityUpperBound() : 0);
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
    return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner,
                                       scanType,
                                       snapshot != null ? snapshot.getVisibilityUpperBound() : 0);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                    InternalScanner scanner, ScanType scanType, CompactionRequest request)
    throws IOException {
    TransactionSnapshot snapshot = cache.getLatestState();
    return new IncrementSummingScanner(region, BATCH_UNLIMITED, scanner,
                                       scanType,
                                       snapshot != null ? snapshot.getVisibilityUpperBound() : 0);
  }

}
