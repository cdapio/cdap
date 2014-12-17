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

package co.cask.cdap.data2.increment.hbase98;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Transforms reads of the stored delta increments into calculated sums for each column.
 */
class IncrementSummingScanner implements RegionScanner {
  private static final Log LOG = LogFactory.getLog(IncrementSummingScanner.class);

  private final HRegion region;
  private final InternalScanner baseScanner;
  private RegionScanner baseRegionScanner;
  private final int batchSize;
  private final ScanType scanType;
  // Highest timestamp, beyond which we cannot aggregate increments during flush and compaction.
  // Increments newer than this may still be visible to running transactions
  private final long compactionUpperBound;
  private List<Cell> cellsToConsume = new ArrayList<Cell>();

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner baseScanner, ScanType scanType) {
    this(region, batchSize, baseScanner, scanType, Long.MAX_VALUE);
  }

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner baseScanner, ScanType scanType,
                          long compationUpperBound) {
    this.region = region;
    this.batchSize = batchSize;
    this.baseScanner = baseScanner;
    if (baseScanner instanceof RegionScanner) {
      this.baseRegionScanner = (RegionScanner) baseScanner;
    }
    this.scanType = scanType;
    this.compactionUpperBound = compationUpperBound;
  }

  @Override
  public HRegionInfo getRegionInfo() {
    return region.getRegionInfo();
  }

  @Override
  public boolean isFilterDone() throws IOException {
    if (baseRegionScanner != null) {
      return baseRegionScanner.isFilterDone();
    }
    throw new IllegalStateException(
      "RegionScanner.isFilterDone() called when the wrapped scanner is not a RegionScanner");
  }

  @Override
  public boolean reseek(byte[] bytes) throws IOException {
    if (baseRegionScanner != null) {
      return baseRegionScanner.reseek(bytes);
    }
    throw new IllegalStateException(
      "RegionScanner.reseek() called when the wrapped scanner is not a RegionScanner");
  }

  @Override
  public long getMaxResultSize() {
    if (baseRegionScanner != null) {
      return baseRegionScanner.getMaxResultSize();
    }
    throw new IllegalStateException(
      "RegionScanner.isFilterDone() called when the wrapped scanner is not a RegionScanner");
  }


  @Override
  public long getMvccReadPoint() {
    if (baseRegionScanner != null) {
      return baseRegionScanner.getMvccReadPoint();
    }
    throw new IllegalStateException(
      "RegionScanner.isFilterDone() called when the wrapped scanner is not a RegionScanner");
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
    return nextInternal(cells, limit);
  }

  private boolean nextInternal(List<Cell> cells, int limit) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("nextInternal called with limit=" + limit);
    }
    Cell previousIncrement = null;
    long runningSum = 0;
    boolean hasMore = true;
    int addedCnt = 0;
    do {
      // 1. fill up results to consume, if needed
      if (cellsToConsume.isEmpty()) {
        hasMore = baseScanner.next(cellsToConsume, limit);
      }
      // compact any delta writes
      for (int i = 0; i < cellsToConsume.size(); i++) {
        Cell cell = cellsToConsume.get(i);
        // 2. check if we've hit the limit for results
        if (limit > 0 && addedCnt >= limit) {
          // haven't reached the end of current cells, so hasMore is true
          cellsToConsume = cellsToConsume.subList(i, cellsToConsume.size() - 1);
          return true;
        }

        // 3. if this is an increment
        if (IncrementHandler.isIncrement(cell) && cell.getTimestamp() < compactionUpperBound) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Found increment for row=" + Bytes.toStringBinary(CellUtil.cloneRow(cell)) + ", " +
                "column=" + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
          }
          if (!sameCell(previousIncrement, cell)) {
            if (previousIncrement != null) {
              // 3a. if different qualifier, and prev qualifier non-null
              // 3ai. emit the previous sum
              if (LOG.isTraceEnabled()) {
                LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement);
              }
              cells.add(newCell(previousIncrement, runningSum));
              addedCnt++;
            }
            previousIncrement = cell;
            runningSum = 0;
          }
          // add this increment to the tally
          runningSum += Bytes.toLong(cell.getValueArray(),
              cell.getValueOffset() + IncrementHandler.DELTA_MAGIC_PREFIX.length);
        } else {
          // 4. otherwise (not an increment or not aggregatable)
          if (previousIncrement != null) {
            boolean skipCurrent = false;
            if (sameCell(previousIncrement, cell)) {
              // 4a. if qualifier matches previous and this is a long, add to running sum, emit
              runningSum += Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
              skipCurrent = true;
            }
            if (LOG.isTraceEnabled()) {
              LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement);
            }
            cells.add(newCell(previousIncrement, runningSum));
            addedCnt++;
            previousIncrement = null;
            runningSum = 0;

            if (skipCurrent) {
              continue;
            }
          }
          // 4b. otherwise emit the current cell
          if (LOG.isTraceEnabled()) {
            LOG.trace("Including raw cell: " + cell);
          }
          cells.add(cell);
          addedCnt++;
        }
      }
      // if we hit the limit before reaching the end of current results, we would have returned above (step 2)
      cellsToConsume.clear();
    } while (hasMore && limit > 0 && addedCnt < limit);
    // emit any left over increment, if we hit the end
    if (previousIncrement != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Including leftover increment: sum=" + runningSum + ", cell=" + previousIncrement);
      }
      cells.add(newCell(previousIncrement, runningSum));
    }

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
    byte[] newValue = Bytes.toBytes(value);
    if (scanType == ScanType.COMPACT_RETAIN_DELETES) {
      newValue = Bytes.add(IncrementHandler.DELTA_MAGIC_PREFIX, newValue);
    }
    return CellUtil.createCell(CellUtil.cloneRow(toCopy), CellUtil.cloneFamily(toCopy),
                               CellUtil.cloneQualifier(toCopy), toCopy.getTimestamp(),
                               KeyValue.Type.Put.getCode(), newValue);
  }

  @Override
  public void close() throws IOException {
    baseScanner.close();
  }
}
