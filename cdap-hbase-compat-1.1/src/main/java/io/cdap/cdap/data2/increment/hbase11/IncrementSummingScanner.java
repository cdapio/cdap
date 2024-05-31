/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.data2.increment.hbase11;

import com.google.common.base.Preconditions;
import io.cdap.cdap.data2.increment.hbase.IncrementHandlerState;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Transforms reads of the stored delta increments into calculated sums for each column.
 */
class IncrementSummingScanner implements RegionScanner {

  private static final Log LOG = LogFactory.getLog(IncrementSummingScanner.class);
  private static final Field SCANNER_CONTEXT_LIMITS_FIELD;
  private static final Method GET_BATCH_METHOD;

  static {
    try {
      Field limitsField = ScannerContext.class.getDeclaredField("limits");
      limitsField.setAccessible(true);
      SCANNER_CONTEXT_LIMITS_FIELD = limitsField;

      Class<?> limitFieldsClass = limitsField.getType();
      Method getBatchMethod = limitFieldsClass.getDeclaredMethod("getBatch");
      getBatchMethod.setAccessible(true);
      GET_BATCH_METHOD = getBatchMethod;
    } catch (Exception e) {
      LOG.error("Failed to get ScannerContext.LimitFields.getBatch method through reflection.");
      throw new IllegalStateException(e);
    }
  }

  private final Region region;
  private final WrappedScanner baseScanner;
  private RegionScanner baseRegionScanner;
  private final int batchSize;
  private final ScanType scanType;
  // Highest timestamp, beyond which we cannot aggregate increments during flush and compaction.
  // Increments newer than this may still be visible to running transactions
  private final long compactionUpperBound;
  // scan start time to use in computing TTL
  private final long oldestTsByTTL;

  IncrementSummingScanner(Region region, int batchSize, InternalScanner internalScanner,
      ScanType scanType) {
    this(region, batchSize, internalScanner, scanType, Long.MAX_VALUE, -1);
  }

  IncrementSummingScanner(Region region, int batchSize, InternalScanner internalScanner,
      ScanType scanType,
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

  /**
   * @return The limit on the number of cells to retrieve on each call to next(). See {@link
   *     Scan#setBatch(int)}
   */
  @Override
  public int getBatch() {
    return batchSize;
  }

  @Override
  public boolean nextRaw(List<Cell> cells) throws IOException {
    return nextRaw(cells, ScannerContext.newBuilder().setBatchLimit(batchSize).build());
  }

  @Override
  public boolean nextRaw(List<Cell> cells, ScannerContext scannerContext) throws IOException {
    return nextInternal(cells, scannerContext);
  }

  @Override
  public boolean next(List<Cell> cells) throws IOException {
    return next(cells, ScannerContext.newBuilder().setBatchLimit(batchSize).build());
  }

  @Override
  public boolean next(List<Cell> cells, ScannerContext scannerContext) throws IOException {
    return nextInternal(cells, scannerContext);
  }

  private boolean nextInternal(List<Cell> cells, ScannerContext scannerContext) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("nextInternal called with limit=" + scannerContext);
    }
    Cell previousIncrement = null;
    long runningSum = 0;
    int addedCnt = 0;
    baseScanner.startNext();
    Cell cell;
    int limit = getBatchLimit(scannerContext);

    while ((cell = baseScanner.peekNextCell(scannerContext)) != null && (limit <= 0
        || addedCnt < limit)) {
      // we use the "peek" semantics so that only once cell is ever emitted per iteration
      // this makes is clearer and easier to enforce that the returned results are <= limit
      if (LOG.isTraceEnabled()) {
        LOG.trace("Checking cell " + cell);
      }
      // any cells visible to in-progress transactions must be kept unchanged
      if (cell.getTimestamp() > compactionUpperBound) {
        if (previousIncrement != null) {
          // if previous increment is present, only emit the previous increment, reset it, and continue.
          // the current cell will be consumed on the next iteration, if we have not yet reached the limit
          if (LOG.isTraceEnabled()) {
            LOG.trace("Including increment: sum=" + runningSum + ", cell=" + previousIncrement);
          }
          cells.add(newCell(previousIncrement, runningSum));
          addedCnt++;
          previousIncrement = null;
          runningSum = 0;

          continue;
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Including cell visible to in-progress, cell=" + cell);
        }
        cells.add(cell);
        addedCnt++;
        baseScanner.nextCell(scannerContext);
        continue;
      }

      // compact any delta writes
      if (IncrementHandler.isIncrement(cell)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "Found increment for row=" + Bytes.toStringBinary(CellUtil.cloneRow(cell)) + ", "
                  + "column=" + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
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
        runningSum += Bytes.toLong(cell.getValueArray(),
            cell.getValueOffset() + IncrementHandlerState.DELTA_MAGIC_PREFIX.length);
      } else {
        // otherwise (not an increment)
        if (previousIncrement != null) {
          if (sameCell(previousIncrement, cell) && !CellUtil.isDelete(cell)) {
            // if qualifier matches previous and this is a long, add to running sum, emit
            runningSum += Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
            // this cell already processed as part of the previous increment's sum, so consume it
            baseScanner.nextCell(scannerContext);
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
      baseScanner.nextCell(scannerContext);
    }
    // emit any left over increment, if we hit the end
    if (previousIncrement != null) {
      // in any situation where we exited due to limit, previousIncrement should already be null
      Preconditions.checkState(getBatch() <= 0 || addedCnt < getBatch(), "addedCnt=%s, limit=%s",
          addedCnt, getBatch());
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Including leftover increment: sum=" + runningSum + ", cell=" + previousIncrement);
      }
      cells.add(newCell(previousIncrement, runningSum));
    }

    boolean hasMore = baseScanner.hasMore();
    if (LOG.isTraceEnabled()) {
      LOG.trace("nextInternal done with limit=" + getBatch() + " hasMore=" + hasMore);
    }
    return hasMore;
  }

  /**
   * Gets the batch limit from the given {@link ScannerContext} through reflection.
   */
  private int getBatchLimit(ScannerContext scannerContext) {
    // We need to to access the batch limit (limit on number of columns) in the ScannerContext.
    // The ScannerContext does not exposes its method to get the limits and has it as an integer inside a classs
    // LimitFields. So get this through reflection. Also, we cannot depend on the limit tracking of the
    // internal scanner as it does not differentiate between an increment cell and a non-increment cell.
    // So, we read all the cells from the internal scanner in batches of limit and do the tracking for columns
    // by ourselves.
    try {
      return (Integer) GET_BATCH_METHOD.invoke(SCANNER_CONTEXT_LIMITS_FIELD.get(scannerContext));
    } catch (Exception e) {
      throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
  }

  private boolean sameCell(Cell first, Cell second) {
    if (first == null && second == null) {
      return true;
    } else if (first == null || second == null) {
      return false;
    }

    return CellUtil.matchingRow(first, second)
        && CellUtil.matchingFamily(first, second)
        && CellUtil.matchingQualifier(first, second);
  }

  private Cell newCell(Cell toCopy, long value) {
    byte[] newValue = Bytes.toBytes(value);
    if (scanType == ScanType.COMPACT_RETAIN_DELETES) {
      newValue = Bytes.add(IncrementHandlerState.DELTA_MAGIC_PREFIX, newValue);
    }
    return CellUtil.createCell(CellUtil.cloneRow(toCopy), CellUtil.cloneFamily(toCopy),
        CellUtil.cloneQualifier(toCopy), toCopy.getTimestamp(),
        KeyValue.Type.Put.getCode(), newValue);
  }

  @Override
  public void close() throws IOException {
    baseScanner.close();
  }

  /**
   * Wraps the underlying store or region scanner in an API that hides the details of calling and
   * managing the buffered batch of results.
   */
  private static class WrappedScanner implements Closeable {

    private boolean hasMore;
    private byte[] currentRow;
    private List<Cell> cellsToConsume = new ArrayList<>();
    private int currentIdx;
    private final InternalScanner scanner;

    WrappedScanner(InternalScanner scanner) {
      this.scanner = scanner;
    }

    /**
     * Called to signal the start of the next() call by the scanner.
     */
    public void startNext() {
      currentRow = null;
    }

    /**
     * Returns the next available cell for the current row, without advancing the pointer.  Calling
     * this method multiple times in a row will continue to return the same cell.
     *
     * @param scannerContext The {@link ScannerContext} instance encapsulating all limits that
     *     should be tracked.
     * @return the next available cell or null if no more cells are available for the current row
     */
    public Cell peekNextCell(ScannerContext scannerContext) throws IOException {
      if (currentIdx >= cellsToConsume.size()) {
        // finished current batch
        cellsToConsume.clear();
        currentIdx = 0;
        hasMore = scanner.next(cellsToConsume, scannerContext);
      }
      Cell cell = null;
      if (currentIdx < cellsToConsume.size()) {
        cell = cellsToConsume.get(currentIdx);
        if (currentRow == null) {
          currentRow = CellUtil.cloneRow(cell);
        } else if (!CellUtil.matchingRow(cell, currentRow)) {
          // moved on to the next row
          // don't consume current cell and signal no more cells for this row
          return null;
        }
      }
      return cell;
    }

    /**
     * Returns the next available cell for the current row and advances the pointer to the next
     * cell.  This method can be called multiple times in a row to advance through all the available
     * cells.
     *
     * @param scannerContext The {@link ScannerContext} instance encapsulating all limits that
     *     should be tracked.
     * @return the next available cell or null if no more cells are available for the current row
     */
    public Cell nextCell(ScannerContext scannerContext) throws IOException {
      Cell cell = peekNextCell(scannerContext);
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
