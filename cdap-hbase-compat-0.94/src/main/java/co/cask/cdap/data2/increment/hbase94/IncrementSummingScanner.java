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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
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
  private final InternalScanner baseScanner;
  private RegionScanner baseRegionScanner;
  private final int batchSize;
  // Highest timestamp, beyond which we cannot aggregate increments during flush and compaction.
  // Increments newer than this may still be visible to running transactions
  private final long compactionUpperBound;

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner baseScanner) {
    this(region, batchSize, baseScanner, Long.MAX_VALUE);
  }

  IncrementSummingScanner(HRegion region, int batchSize, InternalScanner baseScanner, long compationUpperBound) {
    this.region = region;
    this.batchSize = batchSize;
    this.baseScanner = baseScanner;
    if (baseScanner instanceof RegionScanner) {
      this.baseRegionScanner = (RegionScanner) baseScanner;
    }
    this.compactionUpperBound = compationUpperBound;
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
    KeyValue previousIncrement = null;
    long runningSum = 0;
    boolean hasMore;
    int addedCnt = 0;
    do {
      List<KeyValue> tmpCells = new LinkedList<KeyValue>();
      hasMore = baseScanner.next(tmpCells, limit, metric);
      // compact any delta writes
      if (!tmpCells.isEmpty()) {
        for (KeyValue cell : tmpCells) {
          if (limit > 0 && addedCnt >= limit) {
            // haven't reached the end of current cells, so hasMore is true
            return true;
          }

          // 1. if this is an increment
          if (IncrementHandler.isIncrement(cell) && cell.getTimestamp() < compactionUpperBound) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Found increment for row=" + Bytes.toStringBinary(cell.getRow()) + ", " +
                         "column=" + Bytes.toStringBinary(cell.getQualifier()));
            }
            if (!sameCell(previousIncrement, cell)) {
              if (previousIncrement != null) {
                // 1b. if different qualifier, and prev qualifier non-null
                // 1bi. emit the previous sum
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
            runningSum += Bytes.toLong(cell.getBuffer(),
                                       cell.getValueOffset() + IncrementHandler.DELTA_MAGIC_PREFIX.length);
          } else {
            // 2. otherwise (not an increment)
            if (previousIncrement != null) {
              boolean skipCurrent = false;
              if (sameCell(previousIncrement, cell)) {
                // 2a. if qualifier matches previous and this is a long, add to running sum, emit
                runningSum += Bytes.toLong(cell.getBuffer(), cell.getValueOffset());
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
            // 2b. otherwise emit the current cell
            if (LOG.isTraceEnabled()) {
              LOG.trace("Including raw cell " + cell);
            }
            cells.add(cell);
            addedCnt++;
          }
        }
      }
      // NOTE: if limit is -1 (unlimited) then we fetched all cells in one shot, so allow get out of the loop to prevent
      //       fetching next row
      // todo: the baseScanner may have more increment-by-delta cells that has to be merged into one counter value,
      //       which we don't verify by only looking at addedCnt < limit condition. Hence if limit set on scan is less
      //       than number of increment-by-delta cells for a counter it may result into multiple values of same cell to
      //       be returned or partial summation result to be returned. It may not be a problem as we don't yet use
      //       limit on scan explicitly for table datasets. But will cause issues when we do.
      //       See CDAP-971.
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
    return new KeyValue(toCopy.getRow(), toCopy.getFamily(), toCopy.getQualifier(), toCopy.getTimestamp(),
                        Bytes.toBytes(value));
  }

  @Override
  public void close() throws IOException {
    baseScanner.close();
  }
}
