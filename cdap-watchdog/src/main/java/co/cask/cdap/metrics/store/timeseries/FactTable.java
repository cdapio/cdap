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
package co.cask.cdap.metrics.store.timeseries;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.metrics.data.EntityTable;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedSet;
import javax.annotation.Nullable;

/**
 * Table for storing {@link Fact}s.
 *
 * Thread safe as long as the passed into the constructor datasets are thread safe (usually is not the case).
 */
public final class FactTable {
  private static final Logger LOG = LoggerFactory.getLogger(FactTable.class);
  private static final int MAX_ROLL_TIME = 0xfffe;

  private static final Function<byte[], Long> BYTES_TO_LONG = new Function<byte[], Long>() {
    @Override
    public Long apply(byte[] input) {
      return Bytes.toLong(input);
    }
  };

  private static final Function<NavigableMap<byte[], byte[]>, NavigableMap<byte[], Long>>
    TRANSFORM_MAP_BYTE_ARRAY_TO_LONG = new Function<NavigableMap<byte[], byte[]>, NavigableMap<byte[], Long>>() {
    @Override
    public NavigableMap<byte[], Long> apply(NavigableMap<byte[], byte[]> input) {
      return Maps.transformValues(input, BYTES_TO_LONG);
    }
  };

  private final MetricsTable timeSeriesTable;
  private final FactCodec codec;
  private final int resolution;
  private final int rollTime;

  /**
   * Creates an instance of {@link FactTable}.
   *
   * @param timeSeriesTable A table for storing facts informaction.
   * @param entityTable The table for storing tag encoding mappings.
   * @param resolution Resolution in seconds
   * @param rollTime Number of resolution for writing to a new row with a new timebase.
   *                 Meaning the differences between timebase of two consecutive rows divided by
   *                 resolution seconds. It essentially defines how many columns per row in the table.
   *                 This value should be < 65535.
   */
  public FactTable(MetricsTable timeSeriesTable,
            EntityTable entityTable, int resolution, int rollTime) {
    // Two bytes for column name, which is a delta timestamp
    Preconditions.checkArgument(rollTime <= MAX_ROLL_TIME, "Rolltime should be <= " + MAX_ROLL_TIME);

    this.timeSeriesTable = timeSeriesTable;
    this.codec = new FactCodec(entityTable, resolution, rollTime);
    this.resolution = resolution;
    this.rollTime = rollTime;
  }

  public void add(List<Fact> facts) throws Exception {
    // Simply collecting all rows/cols/values that need to be put to the underlying table.
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> gaugesTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> incrementsTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Fact agg : facts) {
      byte[] rowKey = codec.createRowKey(agg.getTagValues(), agg.getMeasureName(), agg.getTimeValue().getTimestamp());
      byte[] column = codec.createColumn(agg.getTimeValue().getTimestamp());

      if (MeasureType.COUNTER == agg.getMeasureType()) {
        inc(incrementsTable, rowKey, column, agg.getTimeValue().getValue());
      } else {
        set(gaugesTable, rowKey, column, Bytes.toBytes(agg.getTimeValue().getValue()));
      }
    }

    NavigableMap<byte[], NavigableMap<byte[], Long>> convertedIncrementsTable =
      Maps.transformValues(incrementsTable, TRANSFORM_MAP_BYTE_ARRAY_TO_LONG);

    NavigableMap<byte[], NavigableMap<byte[], Long>> convertedGaugesTable =
      Maps.transformValues(gaugesTable, TRANSFORM_MAP_BYTE_ARRAY_TO_LONG);

    timeSeriesTable.put(convertedGaugesTable);
    timeSeriesTable.increment(convertedIncrementsTable);
  }

  public FactScanner scan(FactScan scan) throws Exception {
    byte[] startRow = codec.createStartRowKey(scan.getTagValues(), scan.getMeasureName(), scan.getStartTs(), false);
    byte[] endRow = codec.createEndRowKey(scan.getTagValues(), scan.getMeasureName(), scan.getEndTs(), false);
    byte[][] columns = null;
    if (Arrays.equals(startRow, endRow)) {
      // If on the same timebase, we only need subset of columns
      long timeBase = scan.getStartTs() / rollTime * rollTime;
      int startCol = (int) (scan.getStartTs() - timeBase) / resolution;
      int endCol = (int) (scan.getEndTs() - timeBase) / resolution;
      columns = new byte[endCol - startCol + 1][];
      for (int i = 0; i < columns.length; i++) {
        columns[i] = Bytes.toBytes((short) (startCol + i));
      }
    }
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(scan, startRow, false);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Scanning fact table {} with scan: {}; constructed startRow: {}, endRow: {}, fuzzyRowFilter: {}",
                timeSeriesTable, scan, toPrettyLog(startRow), toPrettyLog(endRow), fuzzyRowFilter);
    }

    return new FactScanner(timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter), codec,
                             scan.getStartTs(), scan.getEndTs());
  }

  /**
   * Delete entries in fact table.
   * @param scan
   * @param measurePrefixMatch - we have this to support V2 API for deleting data whose
   *                           measure matches the given prefix.
   * @throws Exception
   */
  public void delete(FactScan scan, boolean measurePrefixMatch) throws Exception {
    byte[] startRow, endRow;

    if (measurePrefixMatch) {
      startRow = codec.createStartRowKey(scan.getTagValues(), null, scan.getStartTs(), false);
      endRow = codec.createEndRowKey(scan.getTagValues(), null, scan.getEndTs(), false);
    } else {
      startRow = codec.createStartRowKey(scan.getTagValues(), scan.getMeasureName(), scan.getStartTs(), false);
      endRow = codec.createEndRowKey(scan.getTagValues(), scan.getMeasureName(), scan.getEndTs(), false);
    }

    byte[][] columns = null;
    Collection<byte[]> rowsToDelete = Lists.newArrayList();
    if (Arrays.equals(startRow, endRow)) {
      // If on the same timebase, we only need subset of columns
      long timeBase = scan.getStartTs() / rollTime * rollTime;
      int startCol = (int) (scan.getStartTs() - timeBase) / resolution;
      int endCol = (int) (scan.getEndTs() - timeBase) / resolution;
      columns = new byte[endCol - startCol + 1][];
      for (int i = 0; i < columns.length; i++) {
        columns[i] = Bytes.toBytes((short) (startCol + i));
      }
    }
    if (!measurePrefixMatch && scan.getMeasureName() != null) {
      endRow = Bytes.stopKeyForPrefix(endRow);
    }
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(scan, startRow, false, measurePrefixMatch);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Deleting fact table {} with scan: {}; constructed startRow: {}, endRow: {}, fuzzyRowFilter: {}",
                timeSeriesTable, scan, toPrettyLog(startRow), toPrettyLog(endRow), fuzzyRowFilter);
    }
    if (!measurePrefixMatch) {
      timeSeriesTable.deleteRange(startRow, endRow, columns, fuzzyRowFilter);
    } else {
      // scan and delete matching measureName prefix
      Scanner scanner = null;
      Row rowResult;
      try {
        scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
        while ((rowResult = scanner.next()) != null) {
          byte[] rowKey = rowResult.getRow();
          if (codec.getMeasureName(rowKey).startsWith(scan.getMeasureName())) {
            //add this to rowsToDelete list
            rowsToDelete.add(rowKey);
            startRow = codec.getNextRowKey(rowResult.getRow(), codec.getTagValues(rowKey).size());
            scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
          }
        }
      } finally {
        scanner.close();
      }
      //finally delete the matched rows
      timeSeriesTable.delete(rowsToDelete);
    }
  }

  /*
   * Given the list of tag values, return sorted collection of tagValues that appear next to the given tagValues
   * (on first position right after given ones) in any of the written facts in specified time range.
   * @param tags - list of tags , we need to return the tags at the next position after these list of tags.
   * @param startTs
   * @param endTs
   * @return Sorted collection of tags
   * @throws Exception
   */
  public Collection<TagValue> findNextAvailableTags(List<TagValue> tags, long startTs, long endTs) throws Exception {
    //todo: push down this logic to server side to reduce RPC calls (CDAP-1421)
    //todo: pass a limit on number of tags returned
    //todo: go over aggregations that match the given tags as first tags in the same order.
    byte[] startRow = codec.createStartRowKey(tags, null, startTs, true);
    byte[] endRow = codec.createEndRowKey(tags, null, endTs, true);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(new FactScan(startTs, endTs, null, tags),
                                                         startRow, true);
    Row rowResult;
    int targetIndex = tags.size();
    SortedSet<TagValue> result = Sets.newTreeSet(new Comparator<TagValue>() {
      @Override
      public int compare(TagValue t1, TagValue t2) {
        int cmp = t1.getTagName().compareTo(t2.getTagName());
        if (cmp != 0) {
          return cmp;
        }
        if (t1.getValue() == null) {
          if (t2.getValue() == null) {
            return 0;
          } else {
            return -1;
          }
        }
        if (t2.getValue() == null) {
          return 1;
        }
        return t1.getValue().compareTo(t2.getValue());
      }
    });

    Scanner scanner = null;
    long startTimeBase = startTs / rollTime * rollTime;
    long endTimeBase = endTs / rollTime * rollTime;
    try {
      scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
      while ((rowResult = scanner.next()) != null) {
        byte[] rowKey = rowResult.getRow();
        // since the timestamp filter is fuzzy and also the aggregate_key is at the begging of rowKey,
        // we could match rows where (ts < startTs or ts > endTs) and the tags at beginning match,
        // we will skip those rows
        if ((codec.getTimestamp(rowKey, codec.createColumn(startTs)) < startTimeBase) ||
          (codec.getTimestamp(rowKey, codec.createColumn(endTs)) > endTimeBase)) {
          continue;
        }
        List<TagValue> tagValues = codec.getTagValues(rowResult.getRow());
        // if the timestamp is within query range and the tag-list matches, we add them to the result
        if (startsWithTags(tags, tagValues)) {
          //add item to result set and increment the start rowKey
          if (tagValues.size() > targetIndex) {
              result.add(tagValues.get(targetIndex));
          }
          startRow = codec.getNextRowKey(rowResult.getRow(), tags.size());
          scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
        }
      }
    } finally {
      scanner.close();
    }

    return result;
  }

  /**
   * Given a list of {@link co.cask.cdap.metrics.store.timeseries.TagValue} and time range,
   * return all measure names of the Facts that have given tagValues and are in the given time range.
   * @param tags - list of tags to match, we return the measureNames that match this tags list.
   * @param startTs
   * @param endTs
   * @return Sorted collection of measureNames
   * @throws Exception
   */
  public Collection<String> getMeasureNames(List<TagValue> tags, long startTs, long endTs) throws Exception {
    //todo: push down this logic to server side to reduce RPC calls (CDAP-1421)
    //todo: pass a limit on number of tags returned
    byte[] startRow = codec.createStartRowKey(tags, null, startTs, true);
    byte[] endRow = codec.createEndRowKey(tags, null, endTs, true);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(new FactScan(startTs, endTs, null, tags),
                                                         startRow, true);
    Row rowResult;
    SortedSet<String> measureNames = Sets.newTreeSet();
    Scanner scanner = null;
    long startTimeBase = startTs / rollTime * rollTime;
    long endTimeBase = endTs / rollTime * rollTime;
    try {
      scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
      while ((rowResult = scanner.next()) != null) {
        byte[] rowKey = rowResult.getRow();
        // since the timestamp filter is fuzzy and also the aggregate_key is at the begging of rowKey,
        // we could match rows where (ts < startTs or ts > endTs) and the tags at beginning match,
        // we will skip those rows
        if ((codec.getTimestamp(rowKey, codec.createColumn(startTs)) < startTimeBase) ||
          (codec.getTimestamp(rowKey, codec.createColumn(endTs)) > endTimeBase)) {
          continue;
        }
        List<TagValue> tagValues = codec.getTagValues(rowResult.getRow());
        // if the timestamp is within query range and the tag-list matches, we add them to the result
        if (startsWithTags(tags, tagValues)) {
          //add item to result set and increment the start rowKey
          measureNames.add(codec.getMeasureName(rowResult.getRow()));
          startRow = codec.getNextRowKey(rowResult.getRow(), tagValues.size());
          scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
        }
      }
    } finally {
      scanner.close();
    }
    return measureNames;
  }

  private boolean startsWithTags(List<TagValue> expected, List<TagValue> actual) {
    if (actual.size() < expected.size()) {
      return false;
    }
    for (int i = 0; i < expected.size(); i++) {
      if (expected.get(i).getValue() != null && !expected.get(i).equals(actual.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private FuzzyRowFilter createFuzzyRowFilter(FactScan scan, byte[] startRow, boolean anyAggGroup) {
    // we need to always use a fuzzy row filter as it is the only one to do the matching of values

    byte[] fuzzyRowMask = codec.createFuzzyRowMask(scan.getTagValues(), scan.getMeasureName(), anyAggGroup);
    // note: we can use startRow, as it will contain all "fixed" parts of the key needed
    return new FuzzyRowFilter(ImmutableList.of(new ImmutablePair<byte[], byte[]>(startRow, fuzzyRowMask)));
  }


  @Nullable
  private FuzzyRowFilter createFuzzyRowFilter(FactScan scan, byte[] startRow, boolean anyAggGroup,
                                              boolean fuzzyMeasureName) {
    // we need to always use a fuzzy row filter as it is the only one to do the matching of values

    byte[] fuzzyRowMask = codec.createFuzzyRowMask(scan.getTagValues(), fuzzyMeasureName ? null : scan.getMeasureName(),
                                                   anyAggGroup);
    // note: we can use startRow, as it will contain all "fixed" parts of the key needed
    return new FuzzyRowFilter(ImmutableList.of(new ImmutablePair<byte[], byte[]>(startRow, fuzzyRowMask)));
  }

  // todo: shouldn't we aggregate "before" writing to FactTable? We could do it really efficient outside
  //       also: the underlying datasets will do aggregation in memory anyways
  private static void inc(NavigableMap<byte[], NavigableMap<byte[], byte[]>> incrementsTable,
                   byte[] rowKey, byte[] column, long value) {
    byte[] oldValue = get(incrementsTable, rowKey, column);
    long newValue = value;
    if (oldValue != null) {
      if (Bytes.SIZEOF_LONG == oldValue.length) {
        newValue = Bytes.toLong(oldValue) + value;
      } else if (Bytes.SIZEOF_INT == oldValue.length) {
        // In 2.4 and older versions we stored it as int
        newValue = Bytes.toInt(oldValue) + value;
      } else {
        // should NEVER happen, unless the table is screwed up manually
        throw new IllegalStateException(
          String.format("Could not parse measure @row %s @column %s value %s as int or long",
                        Bytes.toStringBinary(rowKey), Bytes.toStringBinary(column), Bytes.toStringBinary(oldValue)));
      }

    }
    set(incrementsTable, rowKey, column, Bytes.toBytes(newValue));
  }

  private static byte[] get(NavigableMap<byte[], NavigableMap<byte[], byte[]>> table, byte[] row, byte[] column) {
    NavigableMap<byte[], byte[]> rowMap = table.get(row);
    return rowMap == null ? null : rowMap.get(column);
  }

  private static void set(NavigableMap<byte[], NavigableMap<byte[], byte[]>> table,
                          byte[] row, byte[] column, byte[] value) {
    NavigableMap<byte[], byte[]> rowMap = table.get(row);
    if (rowMap == null) {
      rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      table.put(row, rowMap);
    }

    rowMap.put(column, value);
  }

  private String toPrettyLog(byte[] key) {
    StringBuilder sb = new StringBuilder("{");
    for (byte b : key) {
      String enc = String.valueOf((int) b) + "    ";
      sb.append(enc.substring(0, 5));
    }
    sb.append("}");
    return sb.toString();
  }
}
