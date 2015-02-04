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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  FactTable(MetricsTable timeSeriesTable,
            EntityTable entityTable, int resolution, int rollTime) {
    // Two bytes for column name, which is a delta timestamp
    Preconditions.checkArgument(rollTime <= MAX_ROLL_TIME, "Rolltime should be <= " + MAX_ROLL_TIME);

    this.timeSeriesTable = timeSeriesTable;
    this.codec = new FactCodec(entityTable, resolution, rollTime);
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
    endRow = Bytes.stopKeyForPrefix(endRow);

    // todo: if searching within same row, we can also provide start and end columns or list of columns

    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(scan, startRow, false);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Scanning fact table {} with scan: {}; constructed startRow: {}, endRow: {}, fuzzyRowFilter: {}",
                timeSeriesTable, scan, toPrettyLog(startRow), toPrettyLog(endRow), fuzzyRowFilter);
    }

    return new FactScanner(timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter), codec,
                             scan.getStartTs(), scan.getEndTs());
  }

  /**
   * Given a list of previous tags, we return a list of available tags at the child context in the provided time range.
   * @param previousTags
   * @param startTs
   * @param endTs
   * @return
   * @throws Exception
   */
  public SortedSet<TagValue> getNextTags(List<TagValue> previousTags, long startTs, long endTs) throws Exception {
    byte[] startRow = codec.createStartRowKey(previousTags, null, startTs, true);
    byte[] endRow = codec.createEndRowKey(previousTags, null, endTs, true);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(new FactScan(startTs, endTs, null, previousTags),
                                                                startRow, true);
    Row rowResult;
    int targetIndex = previousTags.size();
    SortedSet<TagValue> nextLevelContexts = Sets.newTreeSet();

    do {
      Scanner scanner = null;
      scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
      rowResult = scanner.next();
      if (rowResult != null) {
        //add item to list and increment the start row
        List<TagValue> tagValues = codec.getTagValues(rowResult.getRow());
        if (tagValues.size() > targetIndex) {
          nextLevelContexts.add(tagValues.get(targetIndex));
        }
        startRow = codec.getNextRowKey(previousTags, rowResult.getRow());
      }
    } while (rowResult != null);

    return nextLevelContexts;
  }

  /**
   * Given a list of {@link co.cask.cdap.metrics.store.timeseries.TagValue} and time range,
   * return the list of metric names with the provided tag value pairs in the given time range.
   * @param previousTags
   * @param startTs
   * @param endTs
   * @return
   * @throws Exception
   */
  public SortedSet<String> getMeasureNames(List<TagValue> previousTags, long startTs, long endTs) throws Exception {
    byte[] startRow = codec.createStartRowKey(previousTags, null, startTs, true);
    byte[] endRow = codec.createEndRowKey(previousTags, null, endTs, true);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(new FactScan(startTs, endTs, null, previousTags),
                                                                startRow, true);
    Row rowResult;
    SortedSet<String> metricNames = Sets.newTreeSet();

    do {
      Scanner scanner = null;
      scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
      rowResult = scanner.next();
      if (rowResult != null) {
        //add item to list and increment the start row
        metricNames.add(codec.getMeasureName(rowResult.getRow()));
        startRow = codec.getNextRowKey(previousTags, rowResult.getRow());
      }
    } while (rowResult != null);

    return metricNames;
  }


  @Nullable
  private FuzzyRowFilter createFuzzyRowFilter(FactScan scan, byte[] startRow, boolean isSearch) {
    // we need to always use a fuzzy row filter as it is the only one to do the matching of values

    byte[] fuzzyRowMask = codec.createFuzzyRowMask(scan.getTagValues(), scan.getMeasureName(), isSearch);
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
