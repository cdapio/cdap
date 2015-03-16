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
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Table for storing {@link Fact}s.
 *
 * Thread safe as long as the passed into the constructor datasets are thread safe (usually is not the case).
 */
public final class FactTable {
  private static final Logger LOG = LoggerFactory.getLogger(FactTable.class);
  private static final int MAX_ROLL_TIME = 0xfffe;

  // hard limits on some ops to stay on safe side
  private static final int MAX_RECORDS_TO_SCAN_DURING_SEARCH = 10 * 1000 * 1000;
  private static final int MAX_SCANS_DURING_SEARCH = 10 * 1000;

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
  // todo: should not be used outside of codec
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
    return new FactScanner(getScanner(scan), codec, scan.getStartTs(), scan.getEndTs());
  }

  private Scanner getScanner(FactScan scan) throws Exception {
    byte[] startRow = codec.createStartRowKey(scan.getTagValues(), scan.getMeasureName(), scan.getStartTs(), false);
    byte[] endRow = codec.createEndRowKey(scan.getTagValues(), scan.getMeasureName(), scan.getEndTs(), false);
    byte[][] columns;
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
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(scan, startRow);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Scanning fact table {} with scan: {}; constructed startRow: {}, endRow: {}, fuzzyRowFilter: {}",
                timeSeriesTable, scan, toPrettyLog(startRow), toPrettyLog(endRow), fuzzyRowFilter);
    }

    return timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
  }

  /**
   * Delete entries in fact table.
   * @param scan specifies deletion criteria
   * @throws Exception
   */
  public void delete(FactScan scan) throws Exception {
    Scanner scanner = getScanner(scan);
    try {
      Row row;
      while ((row = scanner.next()) != null) {
        List<byte[]> columns = Lists.newArrayList();

        boolean exhausted = false;
        for (byte[] column : row.getColumns().keySet()) {
          long ts = codec.getTimestamp(row.getRow(), column);
          if (ts < scan.getStartTs()) {
            continue;
          }

          if (ts > scan.getEndTs()) {
            exhausted = true;
            break;
          }

          columns.add(column);
        }

        timeSeriesTable.delete(row.getRow(), columns.toArray(new byte[columns.size()][]));

        if (exhausted) {
          break;
        }
      }
    } finally {
      scanner.close();
    }
  }

  /**
   * Searches for first non-null valued tags in records that contain given list of tags and match given tag values in
   * given time range. Returned tag values are those that are not defined in given tag values.
   * @param allTagNames list of all tag names to be present in the record
   * @param tagSlice tag values to filter by, {@code null} means any non-null value.
   * @param startTs start of the time range, in seconds
   * @param endTs end of the time range, in seconds
   * @return {@link Set} of {@link TagValue}s
   * @throws Exception
   */
  // todo: pass a limit on number of tagValues returned
  // todo: kinda not cool API when we expect null values in a map...
  public Set<TagValue> findSingleTagValue(List<String> allTagNames, Map<String, String> tagSlice,
                                          long startTs, long endTs) throws Exception {
    // Algorithm, briefly:
    // We scan in the records which have given allTagNames. We use tagSlice as a criteria for scan.
    // If record from the scan has non-null values in the tags which are not specified in tagSlice, we use first of
    // such tag as a value to return.
    // When we find value to return, since we only fill a single tag, we are not interested in drilling down further
    // and instead attempt to fast-forward (jump) to a record that has different value in that tag.
    // Thus we find all results.

    List<TagValue> allTags = Lists.newArrayList();
    List<TagValue> filledTags = Lists.newArrayList();
    List<Integer> tagToFillIndexes = Lists.newArrayList();
    for (int i = 0; i < allTagNames.size(); i++) {
      String tagName = allTagNames.get(i);
      if (!tagSlice.containsKey(tagName)) {
        tagToFillIndexes.add(i);
        allTags.add(new TagValue(tagName, null));
      } else {
        TagValue tagValue = new TagValue(tagName, tagSlice.get(tagName));
        filledTags.add(tagValue);
        allTags.add(tagValue);
      }
    }

    // If provided tags contain all values filled in, there's nothing to look for
    if (tagToFillIndexes.isEmpty()) {
      return Collections.emptySet();
    }

    Set<TagValue> result = Sets.newHashSet();
    int scans = 0;
    int scannedRecords = 0;

    // build a scan
    byte[] startRow = codec.createStartRowKey(allTags, null, startTs, false);
    byte[] endRow = codec.createEndRowKey(allTags, null, endTs, false);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(new FactScan(startTs, endTs, null, allTags), startRow);
    Scanner scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
    scans++;
    try {
      Row rowResult;
      while ((rowResult = scanner.next()) != null) {
        scannedRecords++;
        // todo: make configurable
        if (scannedRecords > MAX_RECORDS_TO_SCAN_DURING_SEARCH) {
          break;
        }
        byte[] rowKey = rowResult.getRow();
        // filter out columns by time range (scan configuration only filters whole rows)
        if (codec.getTimestamp(rowKey, codec.createColumn(startTs)) < startTs) {
          continue;
        }
        if (codec.getTimestamp(rowKey, codec.createColumn(endTs)) > endTs) {
          // we're done with scanner
          break;
        }

        List<TagValue> tagValues = codec.getTagValues(rowResult.getRow());
        // At this point, we know that the record is in right time range and its tags matches given.
        // We try find first non-null valued tag in the record that was not in given tags: we use it to form
        // next drill down suggestion
        int filledIndex = -1;
        for (int index : tagToFillIndexes) {
          // todo: it may be not efficient, if tagValues is not array-backed list: i.e. if access by index is not fast
          TagValue tagValue = tagValues.get(index);
          if (tagValue.getValue() != null) {
            result.add(tagValue);
            filledIndex = index;
            break;
          }
        }

        // Ss soon as we find tag to fill, we are not interested into drilling down further (by contract, we fill
        // single tag value). Thus, we try to jump to the record that has greater value in that tag.
        // todo: fast-forwarding (jumping) should be done on server-side (CDAP-1421)
        if (filledIndex >= 0) {
          scanner.close();
          scanner = null;
          scans++;
          if (scans > MAX_SCANS_DURING_SEARCH) {
            break;
          }
          startRow = codec.getNextRowKey(rowResult.getRow(), filledIndex);
          scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }

    LOG.trace("search for tags completed, scans performed: {}, scanned records: {}", scans, scannedRecords);

    return result;
  }

  /**
   * Finds all measure names of the facts that match given {@link TagValue}s and time range.
   * @param allTagNames list of all tag names to be present in the fact record
   * @param tagSlice tag values to filter by, {@code null} means any non-null value.
   * @param startTs start timestamp, in sec
   * @param endTs end timestamp, in sec
   * @return {@link Set} of measure names
   * @throws Exception
   */
  // todo: pass a limit on number of measures returned
  public Set<String> findMeasureNames(List<String> allTagNames, Map<String, String> tagSlice,
                                      long startTs, long endTs) throws Exception {

    List<TagValue> allTags = Lists.newArrayList();
    for (String tagName : allTagNames) {
      allTags.add(new TagValue(tagName, tagSlice.get(tagName)));
    }

    byte[] startRow = codec.createStartRowKey(allTags, null, startTs, false);
    byte[] endRow = codec.createEndRowKey(allTags, null, endTs, false);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter = createFuzzyRowFilter(new FactScan(startTs, endTs, null, allTags), startRow);

    Set<String> measureNames = Sets.newHashSet();
    int scannedRecords = 0;
    // todo: make configurable
    Scanner scanner = timeSeriesTable.scan(startRow, endRow, null, fuzzyRowFilter);
    try {
      Row rowResult;
      while ((rowResult = scanner.next()) != null) {
        scannedRecords++;
        if (scannedRecords > MAX_RECORDS_TO_SCAN_DURING_SEARCH) {
          break;
        }
        byte[] rowKey = rowResult.getRow();
        // filter out columns by time range (scan configuration only filters whole rows)
        if (codec.getTimestamp(rowKey, codec.createColumn(startTs)) < startTs) {
          continue;
        }
        if (codec.getTimestamp(rowKey, codec.createColumn(endTs)) > endTs) {
          // we're done with scanner
          break;
        }
        measureNames.add(codec.getMeasureName(rowResult.getRow()));
      }
    } finally {
      scanner.close();
    }

    LOG.trace("search for metrics completed, scanned records: {}", scannedRecords);

    return measureNames;
  }

  @Nullable
  private FuzzyRowFilter createFuzzyRowFilter(FactScan scan, byte[] startRow) {
    // we need to always use a fuzzy row filter as it is the only one to do the matching of values

    byte[] fuzzyRowMask = codec.createFuzzyRowMask(scan.getTagValues(), scan.getMeasureName());
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
