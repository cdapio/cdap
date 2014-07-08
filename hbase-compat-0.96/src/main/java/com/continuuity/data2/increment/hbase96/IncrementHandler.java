package com.continuuity.data2.increment.hbase96;

import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 *
 */
public class IncrementHandler extends BaseRegionObserver {
  // prefix bytes used to mark values that are deltas vs. full sums
  private static final byte[] DELTA_MAGIC_PREFIX = new byte[] { 'X', 'D' };
  // expected length for values storing deltas (prefix + increment value)
  private static final int DELTA_FULL_LENGTH = DELTA_MAGIC_PREFIX.length + Bytes.SIZEOF_LONG;

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
    throws IOException {
    super.preGetOp(e, get, results);
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
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
  public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    return super.postScannerOpen(e, scan, s);
  }

  /**
   * Transforms reads of the stored delta increments into calculated sums for each column.
   */
  private static class IncrementSummingScanner implements RegionScanner {
    private final HRegion region;
    private final Scan scan;
    private final RegionScanner baseScanner;
    private final int batchSize;

    private IncrementSummingScanner(HRegion region, Scan scan, RegionScanner baseScanner) {
      this.region = region;
      this.scan = scan;
      this.batchSize = scan.getBatch();
      this.baseScanner = baseScanner;
    }

    @Override
    public HRegionInfo getRegionInfo() {
      return baseScanner.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() throws IOException {
      return baseScanner.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] bytes) throws IOException {
      return baseScanner.reseek(bytes);
    }

    @Override
    public long getMaxResultSize() {
      return baseScanner.getMaxResultSize();
    }

    @Override
    public long getMvccReadPoint() {
      return baseScanner.getMvccReadPoint();
    }

    @Override
    public boolean nextRaw(List<Cell> cells) throws IOException {
      return nextRaw(cells, batchSize);
    }

    @Override
    public boolean nextRaw(List<Cell> cells, int limit) throws IOException {
      return nextInternal(cells, limit);
    }

    @Override
    public boolean next(List<Cell> cells) throws IOException {
      return next(cells, batchSize);
    }

    @Override
    public boolean next(List<Cell> cells, int limit) throws IOException {
      boolean hasMore = false;
      region.startRegionOperation();
      try {
        synchronized (baseScanner) {
          hasMore = nextInternal(cells, limit);
        }
      } finally {
        region.closeRegionOperation();
      }
      return hasMore;
    }

    private boolean nextInternal(List<Cell> cells, int limit) throws IOException {
      byte[] currentQualifierKey = null;
      long runningSum = 0;
      while (true) {
        List<Cell> tmpCells = new LinkedList<Cell>();
        boolean hasMore = baseScanner.nextRaw(tmpCells, limit);
        // compact any delta writes
        if (!tmpCells.isEmpty()) {
          for (Cell cell : tmpCells) {
            // 1. if this is an increment
            // 1a. if qualifier matches previous, add to running sum
            // 1b. if different qualifier, and prev qualifier non-null
            // 1bi. emit the previous sum
            // 1bii. reset running sum and add this
            // 1biii. reset current qualifier
            // 2. otherwise (not an increment)
            // 2a. if qualifier matches previous and this is a long, add to running sum, emit, and continue to next column
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      baseScanner.close();
    }
  }
}
