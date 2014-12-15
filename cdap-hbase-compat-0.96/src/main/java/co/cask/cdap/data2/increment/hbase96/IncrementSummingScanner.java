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
  // Highest timestamp, beyond which we cannot aggregate increments during flush and compaction.
  // Increments newer than this may still be visible to running transactions
  private final long compactionUpperBound;
  private final ScanType scanType;

  // Leftovers from "unfinished" next() invocation:
  // we don't know the number of delta increments for a cell, so we can fetch more than needed from baseScanner, in
  // which case we remember it between next() invocations.
  private Cell previousIncrement = null;
  private long runningSum = 0;

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner baseScanner) {
    this(region, batchSize, baseScanner, ScanType.USER_SCAN, Long.MAX_VALUE);
  }

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner baseScanner,
                          ScanType scanType, long compationUpperBound) {
    this.region = region;
    this.batchSize = batchSize;
    this.baseScanner = baseScanner;
    if (baseScanner instanceof RegionScanner) {
      this.baseRegionScanner = (RegionScanner) baseScanner;
    }
    this.compactionUpperBound = compationUpperBound;
    this.scanType = scanType;
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
    byte[] currentRow = previousIncrement == null ? null : CellUtil.cloneRow(previousIncrement);
    boolean hasMore;
    int addedCnt = 0;
    do {
      List<Cell> tmpCells = new LinkedList<Cell>();
      hasMore = baseScanner.next(tmpCells, limit);
      // compact any delta writes
      if (!tmpCells.isEmpty()) {
        if (currentRow == null) {
          currentRow = CellUtil.cloneRow(tmpCells.get(0));
        }
        for (Cell cell : tmpCells) {
          // NOTE: compactionUpperBound is Long.MAX_VALUE for client scans, so it will squash everything;
          //       while during flush & compaction we can only squash up to it, as may be transactions in progress that
          //       can see only part of the delta-incs group.
          // 1. if this is an increment
          if (IncrementHandler.isIncrement(cell) && cell.getTimestamp() < compactionUpperBound) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Found increment for row=" + Bytes.toStringBinary(CellUtil.cloneRow(cell)) + ", " +
                          "column=" + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)) +
                          ", val=" + Bytes.toStringBinary(CellUtil.cloneValue(cell)));
            }
            if (!sameCell(previousIncrement, cell)) {
              if (previousIncrement != null) {
                // 1b. if different qualifier, and prev qualifier non-null
                // 1bi. emit the previous sum
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement +
                              ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(previousIncrement)));
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
            if (LOG.isTraceEnabled()) {
              LOG.trace("Increased runSum: sum=" + runningSum + ", cell=" + cell +
                          ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(cell)));
            }
          } else {
            // 2. otherwise (not an increment)
            if (previousIncrement != null) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement +
                            ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(previousIncrement)));
              }
              cells.add(newCell(previousIncrement, runningSum));
              addedCnt++;
              previousIncrement = null;
              runningSum = 0;
            }
            // emit the current cell as is
            if (LOG.isTraceEnabled()) {
              LOG.trace("Including raw cell " + cell + ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(cell)));
            }
            cells.add(cell);
            addedCnt++;
          }
        }
      }

      // we want to exit if we reached next row
      if (previousIncrement != null && !CellUtil.matchingRow(previousIncrement, currentRow)) {
        // we have more cells, but of the different row
        return true;
      }

      // NOTE: if limit is -1 (unlimited) then we fetched all cells in one shot, so allow get out of the loop to prevent
      //       fetching next row
    } while (hasMore && limit > 0 && addedCnt < limit);

    // No leftover, simply return if there's anything left in baseScanner.
    if (previousIncrement == null) {
      return hasMore;
    }

    // At this point, we
    // * either added "enough" (up to limit) or
    // * there's nothing left in baseScanner (hasMore=false) or
    // * limit is -1, hasMore can be true or false, but there are no cells for this row left in scanner (since we
    //   fetched all by passing limit=-1 to baseScanner)
    // And we know there's leftover.
    if (!hasMore || limit < 0) {
      // A. nothing left in base scanner for current row
      //    see if we can add leftover or need to return back for it in next iteration
      if (limit < 0 || addedCnt < limit) {
        // A-1. has not reached limit, can add last one, there's nothing to be left in this case.
        if (LOG.isTraceEnabled()) {
          LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement +
                      ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(previousIncrement)));
        }
        cells.add(newCell(previousIncrement, runningSum));
        previousIncrement = null;
        runningSum = 0;
        return hasMore;
      }
      // A-2. has reached limit. We have to ask to return for leftover in next batch to comply with given limit
      return true;
    }

    // B. There's more in base scanner, we have leftover which we'll attempt to merge with those left in base scanner
    //    in next iteration
    return true;
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
    // we want to give normal long value to the user scan, while for compaction we want to write summation as delta
    if (scanType == ScanType.USER_SCAN) {
      return CellUtil.createCell(CellUtil.cloneRow(toCopy), CellUtil.cloneFamily(toCopy),
                                 CellUtil.cloneQualifier(toCopy), toCopy.getTimestamp(),
                                 KeyValue.Type.Put.getCode(), Bytes.toBytes(value));
    } else {
      return IncrementHandler.createDeltaIncrement(CellUtil.cloneRow(toCopy), CellUtil.cloneFamily(toCopy),
                                                   CellUtil.cloneQualifier(toCopy), toCopy.getTimestamp(),
                                                   KeyValue.Type.Put.getCode(), Bytes.toBytes(value));
    }
  }

  @Override
  public void close() throws IOException {
    baseScanner.close();
  }
}
