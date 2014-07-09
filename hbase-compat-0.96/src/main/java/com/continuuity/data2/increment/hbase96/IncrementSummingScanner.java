package com.continuuity.data2.increment.hbase96;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Transforms reads of the stored delta increments into calculated sums for each column.
 */
class IncrementSummingScanner implements RegionScanner {
  private static final Log LOG = LogFactory.getLog(IncrementSummingScanner.class);

  private final HRegion region;
  private final Scan scan;
  private final RegionScanner baseScanner;
  private final int batchSize;

  IncrementSummingScanner(HRegion region, Scan scan, RegionScanner baseScanner) {
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
    Cell previousIncrement = null;
    long runningSum = 0;
    boolean hasMore;
    int addedCnt = 0;
    do {
      List<Cell> tmpCells = new LinkedList<Cell>();
      hasMore = baseScanner.nextRaw(tmpCells, limit);
      // compact any delta writes
      if (!tmpCells.isEmpty()) {
        for (Cell cell : tmpCells) {
          if (limit > 0 && addedCnt >= limit) {
            // haven't reached the end of current cells, so hasMore is true
            return true;
          }

          // 1. if this is an increment
          if (IncrementHandler.isIncrement(cell)) {
            LOG.info("Found increment for row=" + Bytes.toStringBinary(CellUtil.cloneRow(cell)) + ", " +
                       "column=" + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
            if (!sameCell(previousIncrement, cell)) {
              if (previousIncrement != null) {
                // 1b. if different qualifier, and prev qualifier non-null
                // 1bi. emit the previous sum
                cells.add(newCell(previousIncrement, runningSum));
                addedCnt++;
              }
              previousIncrement = cell;
              runningSum = 0;
            }
            runningSum += Bytes.toLong(cell.getValueArray(),
                                       cell.getValueOffset() + IncrementHandler.DELTA_MAGIC_PREFIX.length);
            } else if (sameCell(previousIncrement, cell)) {
              // 1a. if qualifier matches previous, add to running sum
              runningSum += Bytes.toLong(cell.getValueArray(),
                                         cell.getValueOffset() + IncrementHandler.DELTA_MAGIC_PREFIX.length);
            } else {
              // 1bii. reset running sum to this, reset previous entry
              previousIncrement = cell;
              runningSum = Bytes.toLong(cell.getValueArray(),
                                        cell.getValueOffset() + IncrementHandler.DELTA_MAGIC_PREFIX.length);
            }
          } else {
            // 2. otherwise (not an increment)
            if (previousIncrement != null) {
              boolean skipCurrent = false;
              if (CellUtil.matchingRow(previousIncrement, cell) &&
                CellUtil.matchingFamily(previousIncrement, cell) &&
                CellUtil.matchingQualifier(previousIncrement, cell)) {
                // 2a. if qualifier matches previous and this is a long, add to running sum, emit
                runningSum += Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
                skipCurrent = true;
              }
              cells.add(newCell(previousIncrement, runningSum));
              addedCnt++;
              previousIncrement = null;
              runningSum = 0;

              if (skipCurrent) {
                continue;
              }
            }
            // 2b. otherwise emit the current cell
            cells.add(cell);
            addedCnt++;
          }
        }
        // emit any left over increment, if we hit the end
        if (!hasMore && previousIncrement != null) {
          cells.add(newCell(previousIncrement, runningSum));
        }
      }
    } while (hasMore && (limit <=0 || addedCnt < limit));

    return hasMore;
  }

  private boolean sameCell(Cell first, Cell second) {
    if (first == null && second == null) {
      return true;
    } else if (first == null || second == null) {
      return false;
    }

    return CellUtil.matchingRow(first, second) &&
      CellUtil.matchingFamily(first, second) &&
      CellUtil.matchingQualifier(first, second);
  }
  private Cell newCell(Cell toCopy, long value) {
    return CellUtil.createCell(CellUtil.cloneRow(toCopy), CellUtil.cloneFamily(toCopy),
                               CellUtil.cloneQualifier(toCopy), toCopy.getTimestamp(),
                               KeyValue.Type.Put.getCode(), Bytes.toBytes(value));
  }

  @Override
  public void close() throws IOException {
    baseScanner.close();
  }
}
