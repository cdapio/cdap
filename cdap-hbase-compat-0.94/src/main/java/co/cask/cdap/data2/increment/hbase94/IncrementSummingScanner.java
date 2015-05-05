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

package co.cask.cdap.data2.increment.hbase94;

import co.cask.cdap.data2.increment.hbase.IncrementHandlerState;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transforms reads of the stored delta increments into calculated sums for each column.
 */
class IncrementSummingScanner implements RegionScanner {
  private static final Log LOG = LogFactory.getLog(IncrementSummingScanner.class);

  private final HRegion region;
  private final WrappedScanner baseScanner;
  private RegionScanner baseRegionScanner;
  private final int batchSize;
  private final ScanType scanType;
  // Highest timestamp, beyond which we cannot aggregate increments during flush and compaction.
  // Increments newer than this may still be visible to running transactions
  private final long compactionUpperBound;
  // scan start time to use in computing TTL
  private final long oldestTsByTTL;

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner internalScanner, ScanType scanType) {
    this(region, batchSize, internalScanner, scanType, Long.MAX_VALUE, -1);
  }

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner internalScanner, ScanType scanType,
                          long compationUpperBound, long oldestTsByTTL) {
    this.region = region;
    this.batchSize = batchSize;
    this.baseScanner = new WrappedScanner(internalScanner);
    if (internalScanner instanceof RegionScanner) {
      this.baseRegionScanner = (RegionScanner) internalScanner;
    }
    this.scanType = scanType;
    this.compactionUpperBound = compationUpperBound;
    this.oldestTsByTTL = oldestTsByTTL;
  }

  @Override
  public HRegionInfo getRegionInfo() {
    return region.getRegionInfo();
  }

  @Override
  public boolean isFilterDone() {
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
  public long getMvccReadPoint() {
    if (baseRegionScanner != null) {
      return baseRegionScanner.getMvccReadPoint();
    }
    throw new IllegalStateException(
      "RegionScanner.isFilterDone() called when the wrapped scanner is not a RegionScanner");
  }

  @Override
  public boolean nextRaw(List<KeyValue> cells, String metric) throws IOException {
    return nextRaw(cells, batchSize, metric);
  }

  @Override
  public boolean nextRaw(List<KeyValue> cells, int limit, String metric) throws IOException {
    return nextInternal(cells, limit, metric);
  }

  @Override
  public boolean next(List<KeyValue> cells) throws IOException {
    return next(cells, batchSize, null);
  }

  @Override
  public boolean next(List<KeyValue> cells, int limit) throws IOException {
    return next(cells, limit, null);
  }

  @Override
  public boolean next(List<KeyValue> cells, String metric) throws IOException {
    return next(cells, batchSize, metric);
  }

  @Override
  public boolean next(List<KeyValue> cells, int limit, String metric) throws IOException {
    return nextInternal(cells, limit, metric);
  }

  private boolean nextInternal(List<KeyValue> cells, int limit, String metric) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("nextInternal called with limit=" + limit);
    }
    KeyValue previousIncrement = null;
    long runningSum = 0;
    int addedCnt = 0;
    baseScanner.startNext();
    KeyValue cell = null;
    while ((cell = baseScanner.peekNextCell(limit)) != null && (limit <= 0 || addedCnt < limit)) {
      // we use the "peek" semantics so that only once cell is ever emitted per iteration
      // this makes is clearer and easier to enforce that the returned results are <= limit
      if (LOG.isTraceEnabled()) {
        LOG.trace("Checking cell " + cell);
      }
      // any cells visible to in-progress transactions must be kept unchanged
      if (cell.getTimestamp() > compactionUpperBound) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Including cell visible to in-progress, cell=" + cell);
        }
        cells.add(cell);
        addedCnt++;
        baseScanner.nextCell(limit);
        continue;
      }

      // compact any delta writes
      if (IncrementHandler.isIncrement(cell)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found increment for row=" + Bytes.toStringBinary(cell.getRow()) + ", " +
              "column=" + Bytes.toStringBinary(cell.getQualifier()));
        }
        if (!sameCell(previousIncrement, cell)) {
          if (previousIncrement != null) {
            // if different qualifier, and prev qualifier non-null
            // emit the previous sum
            if (LOG.isTraceEnabled()) {
              LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement);
            }
            cells.add(newCell(previousIncrement, runningSum));
            previousIncrement = null;
            addedCnt++;
            // continue without advancing, current cell will be consumed on the next iteration
            continue;
          }
          previousIncrement = cell;
          runningSum = 0;
        }
        // add this increment to the tally
        runningSum += Bytes.toLong(cell.getBuffer(),
            cell.getValueOffset() + IncrementHandlerState.DELTA_MAGIC_PREFIX.length);
      } else {
        // otherwise (not an increment)
        if (previousIncrement != null) {
          if (sameCell(previousIncrement, cell) && !KeyValue.isDelete(cell.getType())) {
            // if qualifier matches previous and this is a long, add to running sum, emit
            runningSum += Bytes.toLong(cell.getBuffer(), cell.getValueOffset());
            // this cell already processed as part of the previous increment's sum, so consume it
            baseScanner.nextCell(limit);
          }
          if (LOG.isTraceEnabled()) {
            LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement);
          }
          // if this put is a different cell from the previous increment, then
          // we only emit the previous increment, reset it, and continue.
          // the current cell will be consumed on the next iteration, if we have not yet reached the limit
          cells.add(newCell(previousIncrement, runningSum));
          addedCnt++;
          previousIncrement = null;
          runningSum = 0;

          continue;
        }
        // otherwise emit the current cell
        if (LOG.isTraceEnabled()) {
          LOG.trace("Including raw cell: " + cell);
        }

        // apply any configured TTL
        if (cell.getTimestamp() > oldestTsByTTL) {
          cells.add(cell);
          addedCnt++;
        }
      }
      // if we made it this far, consume the current cell
      baseScanner.nextCell(limit);
    }
    // emit any left over increment, if we hit the end
    if (previousIncrement != null) {
      // in any situation where we exited due to limit, previousIncrement should already be null
      Preconditions.checkState(limit <= 0 || addedCnt < limit);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Including leftover increment: sum=" + runningSum + ", cell=" + previousIncrement);
      }
      cells.add(newCell(previousIncrement, runningSum));
    }

    return baseScanner.hasMore();
  }

  private boolean sameCell(KeyValue first, KeyValue second) {
    if (first == null && second == null) {
      return true;
    } else if (first == null || second == null) {
      return false;
    }

    return Bytes.equals(
        first.getBuffer(), first.getKeyOffset(), first.getKeyLength() - KeyValue.TIMESTAMP_SIZE,
        second.getBuffer(), second.getKeyOffset(), second.getKeyLength() - KeyValue.TIMESTAMP_SIZE);
  }

  private KeyValue newCell(KeyValue toCopy, long value) {
    byte[] newValue = Bytes.toBytes(value);
    if (scanType != ScanType.USER_SCAN && scanType != ScanType.MAJOR_COMPACT) {
      newValue = Bytes.add(IncrementHandlerState.DELTA_MAGIC_PREFIX, newValue);
    }
    return new KeyValue(toCopy.getRow(), toCopy.getFamily(), toCopy.getQualifier(), toCopy.getTimestamp(),
                        newValue);
  }

  @Override
  public void close() throws IOException {
    baseScanner.close();
  }

  /**
   * Wraps the underlying store or region scanner in an API that hides the details of calling and managing the
   * buffered batch of results.
   */
  private static class WrappedScanner implements Closeable {
    private boolean hasMore;
    private byte[] currentRow;
    private List<KeyValue> cellsToConsume = new ArrayList<KeyValue>();
    private int currentIdx;
    private final InternalScanner scanner;

    public WrappedScanner(InternalScanner scanner) {
      this.scanner = scanner;
    }

    /**
     * Called to signal the start of the next() call by the scanner.
     */
    public void startNext() {
      currentRow = null;
    }

    /**
     * Returns the next available cell for the current row, without advancing the pointer.  Calling this method
     * multiple times in a row will continue to return the same cell.
     *
     * @param limit the limit of number of cells to return if the next batch must be fetched by the wrapped scanner
     * @return the next available cell or null if no more cells are available for the current row
     * @throws IOException
     */
    public KeyValue peekNextCell(int limit) throws IOException {
      if (currentIdx >= cellsToConsume.size()) {
        // finished current batch
        cellsToConsume.clear();
        currentIdx = 0;
        hasMore = scanner.next(cellsToConsume, limit);
      }
      KeyValue cell = null;
      if (currentIdx < cellsToConsume.size()) {
        cell = cellsToConsume.get(currentIdx);
        if (currentRow == null) {
          currentRow = cell.getRow();
        } else if (!Bytes.equals(cell.getBuffer(), cell.getRowOffset(), cell.getRowLength(),
            currentRow, 0, currentRow.length)) {
          // moved on to the next row
          // don't consume current cell and signal no more cells for this row
          return null;
        }
      }
      return cell;
    }

    /**
     * Returns the next available cell for the current row and advances the pointer to the next cell.  This method
     * can be called multiple times in a row to advance through all the available cells.
     *
     * @param limit the limit of number of cells to return if the next batch must be fetched by the wrapped scanner
     * @return the next available cell or null if no more cells are available for the current row
     * @throws IOException
     */
    public KeyValue nextCell(int limit) throws IOException {
      KeyValue cell = peekNextCell(limit);
      if (cell != null) {
        currentIdx++;
      }
      return cell;
    }

    /**
     * Returns whether or not the underlying scanner has more rows.
     */
    public boolean hasMore() {
      return currentIdx < cellsToConsume.size() ? true : hasMore;
    }

    @Override
    public void close() throws IOException {
      scanner.close();
    }
  }
}
