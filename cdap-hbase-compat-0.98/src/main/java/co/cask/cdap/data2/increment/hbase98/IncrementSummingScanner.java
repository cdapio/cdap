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
import java.util.Iterator;
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

  // Left over non-processed cells from previous scanner next() invocation.
  private List<Cell> leftOverCells = null;

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
    LOG.info("upperVisBound: " + compationUpperBound);
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
    // Logic, briefly:
    // * get cells from base scanner in batches
    // * iterate over the batch, add up to the cells to be returned
    // * once hit limit or next row: keep unprocessed cells, we'll start with them next time nextInternal is called
    // * do until nothing left in scan

    // to track if anything left in scanner
    boolean hasMore = true;
    // to track if still within a row: all returned cells must be within same row
    byte[] currentRow = null;

    // an counter value may be a sum of multiple delta-increment cells,
    // so keeping intermediate running counting in this variables
    Cell runningIncrement = null;
    long runningSum = 0;

    // already added cells to the cells
    int addedCnt = 0;

    while (true) {
      List<Cell> tmpCells;
      if (leftOverCells != null) {
        tmpCells = leftOverCells;
        leftOverCells = null;
      } else {
        tmpCells = new LinkedList<Cell>();
        hasMore = baseScanner.next(tmpCells, limit);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Got " + tmpCells.size() + " from baseScanner, hasMore: " + hasMore + ", limit: " + limit);
        }
      }

      if (tmpCells.isEmpty()) {
        // we are done: adding running increment if any, and exit
        hasMore = false;
        break;
      }

      if (currentRow == null) {
        currentRow = CellUtil.cloneRow(tmpCells.get(0));
      } else {
        // if we crossed the row boundaries, store the newly fetched into leftovers and exit (add running if any before)
        if (!CellUtil.matchingRow(tmpCells.get(0), currentRow)) {
          leftOverCells = tmpCells;
          hasMore = true;
          break;
        }
      }

      // iterate over, while compacting what is possible
      Iterator<Cell> it = tmpCells.iterator();
      boolean storeLeftOverAndExit = false;
      Cell cell = null;
      while (it.hasNext()) {
        cell = it.next();
        boolean includeRaw = false;
        // NOTE: compactionUpperBound is Long.MAX_VALUE for client scans, so it will squash everything;
        //       while during flush & compaction we can only squash up to it, as may be transactions in progress that
        //       can see only part of the delta-incs group.
        // 1. if this is an increment & we can squash it
        if (IncrementHandler.isIncrement(cell) && cell.getTimestamp() < compactionUpperBound) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Found increment for row=" + Bytes.toStringBinary(CellUtil.cloneRow(cell)) + ", " +
                       "column=" + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)) +
                       ", val=" + Bytes.toStringBinary(CellUtil.cloneValue(cell)));
          }
          if (!sameCell(runningIncrement, cell)) {
            if (runningIncrement != null) {
              // 1b. if different qualifier, and prev qualifier non-null
              // 1bi. emit the previous sum
              if (LOG.isTraceEnabled()) {
                LOG.trace("Including increment: sum=" + runningSum + ", cell=" + runningIncrement +
                           ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(runningIncrement)));
              }
              cells.add(newCell(runningIncrement, runningSum));
              addedCnt++;
            }
            runningIncrement = cell;
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
          // 2. otherwise (not an increment that we can squash)
          if (runningIncrement != null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Including increment: sum=" + runningSum + ", cell=" + runningIncrement +
                        ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(runningIncrement)));
            }
            cells.add(newCell(runningIncrement, runningSum));
            addedCnt++;
            runningIncrement = null;
            runningSum = 0;
          }

          includeRaw = true;
        }

        if (limit > 0 && addedCnt >= limit) {
          storeLeftOverAndExit = true;
          break;
        }

        if (includeRaw) {
          // emit the current cell as is
          if (LOG.isTraceEnabled()) {
            LOG.trace("Including raw cell " + cell + ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(cell)));
          }
          cells.add(cell);
          addedCnt++;
        }
      }

      if (storeLeftOverAndExit) {
        leftOverCells = new LinkedList<Cell>();
        // we did not process last one: exited due to limit
        if (cell != null) {
          leftOverCells.add(cell);
        }
        while (it.hasNext()) {
          leftOverCells.add(it.next());
        }

        // we can throw away runningIncrement as we remembered the cell in leftOverCells
        return hasMore || !leftOverCells.isEmpty();
      }
    }

    // adding any running increment and exit
    if (runningIncrement != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Including increment: sum=" + runningSum + ", cell=" + runningIncrement +
                    ", val: " + Bytes.toStringBinary(CellUtil.cloneValue(runningIncrement)));
      }
      cells.add(newCell(runningIncrement, runningSum));
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
