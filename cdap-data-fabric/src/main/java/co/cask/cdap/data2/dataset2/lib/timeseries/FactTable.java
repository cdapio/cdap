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
package co.cask.cdap.data2.dataset2.lib.timeseries;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.Measurement;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.metrics.MetricsCollector;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
public final class FactTable implements Closeable {
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
  private final EntityTable entityTable;
  private final FactCodec codec;
  private final int resolution;
  // todo: should not be used outside of codec
  private final int rollTime;

  private final String putCountMetric;
  private final String incrementCountMetric;

  @Nullable
  private MetricsCollector metrics;

  /**
   * Creates an instance of {@link FactTable}.
   *
   * @param timeSeriesTable A table for storing facts information.
   * @param entityTable The table for storing dimension encoding mappings.
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

    this.entityTable = entityTable;
    this.timeSeriesTable = timeSeriesTable;
    this.codec = new FactCodec(entityTable, resolution, rollTime);
    this.resolution = resolution;
    this.rollTime = rollTime;
    this.putCountMetric = "factTable." + resolution + ".put.count";
    this.incrementCountMetric = "factTable." + resolution + ".increment.count";
  }

  public void setMetricsCollector(MetricsCollector metrics) {
    this.metrics = metrics;
  }

  public void add(List<Fact> facts) {
    // Simply collecting all rows/cols/values that need to be put to the underlying table.
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> gaugesTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> incrementsTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Fact fact : facts) {
      for (Measurement measurement : fact.getMeasurements()) {
        byte[] rowKey = codec.createRowKey(fact.getDimensionValues(), measurement.getName(), fact.getTimestamp());
        byte[] column = codec.createColumn(fact.getTimestamp());

        if (MeasureType.COUNTER == measurement.getType()) {
          inc(incrementsTable, rowKey, column, measurement.getValue());
        } else {
          set(gaugesTable, rowKey, column, Bytes.toBytes(measurement.getValue()));
        }
      }
    }

    NavigableMap<byte[], NavigableMap<byte[], Long>> convertedIncrementsTable =
      Maps.transformValues(incrementsTable, TRANSFORM_MAP_BYTE_ARRAY_TO_LONG);

    NavigableMap<byte[], NavigableMap<byte[], Long>> convertedGaugesTable =
      Maps.transformValues(gaugesTable, TRANSFORM_MAP_BYTE_ARRAY_TO_LONG);

    // todo: replace with single call, to be able to optimize rpcs in underlying table
    timeSeriesTable.put(convertedGaugesTable);
    timeSeriesTable.increment(convertedIncrementsTable);
    if (metrics != null) {
      metrics.increment(putCountMetric, convertedGaugesTable.size());
      metrics.increment(incrementCountMetric, convertedIncrementsTable.size());
    }
  }

  private class MeasureNameComparator implements Comparator<String> {
    private final Map<String, Long> measureNameToEntityIdMap;

    private MeasureNameComparator(Map<String, Long> measureNameToEntityIdMap) {
      this.measureNameToEntityIdMap = measureNameToEntityIdMap;
    }

    @Override
    public int compare(String first, String second) {
      return Long.compare(measureNameToEntityIdMap.get(first), measureNameToEntityIdMap.get(second));
    }
  }

  public FactScanner scan(FactScan scan) {
    return new FactScanner(getScanner(scan), codec, scan.getStartTs(), scan.getEndTs(), scan.getMeasureNames());
  }

  private List<String> getSortedMeasures(Collection<String> measures) {
    Map<String, Long> measureToEntityMap = new HashMap<>();
    List<String> measureNames = new ArrayList<>();

    for (String measureName : measures) {
      measureNames.add(measureName);
      measureToEntityMap.put(measureName, codec.getMeasureEntityId(measureName));
    }
    // sort the list
    Collections.sort(measureNames, new MeasureNameComparator(measureToEntityMap));
    return measureNames;
  }

  private Scanner getScanner(FactScan scan) {

    // sort the measures based on their entity ids and based on that get the start and end row key metric names
    List<String> measureNames = getSortedMeasures(scan.getMeasureNames());

    byte[] startRow = codec.createStartRowKey(scan.getDimensionValues(),
                                              measureNames.isEmpty() ? null : measureNames.get(0),
                                              scan.getStartTs(), false);
    byte[] endRow = codec.createEndRowKey(scan.getDimensionValues(),
                                          measureNames.isEmpty() ? null : measureNames.get(measureNames.size() - 1),
                                          scan.getEndTs(), false);
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
    FuzzyRowFilter fuzzyRowFilter =
      measureNames.isEmpty() ? createFuzzyRowFilter(scan, startRow) : createFuzzyRowFilter(scan, measureNames);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Scanning fact table {} with scan: {}; constructed startRow: {}, endRow: {}, fuzzyRowFilter: {}",
                timeSeriesTable, scan, toPrettyLog(startRow), toPrettyLog(endRow), fuzzyRowFilter);
    }

    LOG.info("####### FactTable start: {}", Bytes.toHexString(startRow));
    LOG.info("####### FactTable stop: {}", Bytes.toHexString(endRow));

    return timeSeriesTable.scan(startRow, endRow, fuzzyRowFilter);
  }

  /**
   * Delete entries in fact table.
   * @param scan specifies deletion criteria
   */
  public void delete(FactScan scan) {
    try (Scanner scanner = getScanner(scan)) {
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

        // todo: do deletes efficiently, in batches, not one-by-one
        timeSeriesTable.delete(row.getRow(), columns.toArray(new byte[columns.size()][]));

        if (exhausted) {
          break;
        }
      }
    }
  }

  /**
   * Searches for first non-null valued dimensions in records that contain given list of dimensions and match given
   * dimension values in given time range. Returned dimension values are those that are not defined in given
   * dimension values.
   * @param allDimensionNames list of all dimension names to be present in the record
   * @param dimensionSlice dimension values to filter by, {@code null} means any non-null value.
   * @param startTs start of the time range, in seconds
   * @param endTs end of the time range, in seconds
   * @return {@link Set} of {@link DimensionValue}s
   */
  // todo: pass a limit on number of dimensionValues returned
  // todo: kinda not cool API when we expect null values in a map...
  public Set<DimensionValue> findSingleDimensionValue(List<String> allDimensionNames,
                                                      Map<String, String> dimensionSlice,
                                                      long startTs, long endTs) {
    // Algorithm, briefly:
    // We scan in the records which have given allDimensionNames. We use dimensionSlice as a criteria for scan.
    // If record from the scan has non-null values in the dimensions which are not specified in dimensionSlice,
    // we use first of such dimension as a value to return.
    // When we find value to return, since we only fill a single dimension, we are not interested in drilling down
    // further and instead attempt to fast-forward (jump) to a record that has different value in that dimension.
    // Thus we find all results.

    List<DimensionValue> allDimensions = Lists.newArrayList();
    List<DimensionValue> filledDimension = Lists.newArrayList();
    List<Integer> dimToFillIndexes = Lists.newArrayList();
    for (int i = 0; i < allDimensionNames.size(); i++) {
      String dimensionName = allDimensionNames.get(i);
      if (!dimensionSlice.containsKey(dimensionName)) {
        dimToFillIndexes.add(i);
        allDimensions.add(new DimensionValue(dimensionName, null));
      } else {
        DimensionValue dimensionValue = new DimensionValue(dimensionName, dimensionSlice.get(dimensionName));
        filledDimension.add(dimensionValue);
        allDimensions.add(dimensionValue);
      }
    }

    // If provided dimensions contain all values filled in, there's nothing to look for
    if (dimToFillIndexes.isEmpty()) {
      return Collections.emptySet();
    }

    Set<DimensionValue> result = Sets.newHashSet();
    int scans = 0;
    int scannedRecords = 0;

    // build a scan
    byte[] startRow = codec.createStartRowKey(allDimensions, null, startTs, false);
    byte[] endRow = codec.createEndRowKey(allDimensions, null, endTs, false);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter =
      createFuzzyRowFilter(new FactScan(startTs, endTs, ImmutableList.<String>of(), allDimensions), startRow);
    Scanner scanner = timeSeriesTable.scan(startRow, endRow, fuzzyRowFilter);
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

        List<DimensionValue> dimensionValues = codec.getDimensionValues(rowResult.getRow());
        // At this point, we know that the record is in right time range and its dimensions matches given.
        // We try find first non-null valued dimension in the record that was not in given dimensions: we use it to form
        // next drill down suggestion
        int filledIndex = -1;
        for (int index : dimToFillIndexes) {
          // todo: it may be not efficient, if dimensionValues is not array-backed list: i.e. if access by index is
          //       not fast
          DimensionValue dimensionValue = dimensionValues.get(index);
          if (dimensionValue.getValue() != null) {
            result.add(dimensionValue);
            filledIndex = index;
            break;
          }
        }

        // Ss soon as we find dimension to fill, we are not interested into drilling down further (by contract, we fill
        // single dimension value). Thus, we try to jump to the record that has greater value in that dimension.
        // todo: fast-forwarding (jumping) should be done on server-side (CDAP-1421)
        if (filledIndex >= 0) {
          scanner.close();
          scanner = null;
          scans++;
          if (scans > MAX_SCANS_DURING_SEARCH) {
            break;
          }
          startRow = codec.getNextRowKey(rowResult.getRow(), filledIndex);
          scanner = timeSeriesTable.scan(startRow, endRow, fuzzyRowFilter);
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }

    LOG.trace("search for dimensions completed, scans performed: {}, scanned records: {}", scans, scannedRecords);

    return result;
  }

  /**
   * Finds all measure names of the facts that match given {@link DimensionValue}s and time range.
   * @param allDimensionNames list of all dimension names to be present in the fact record
   * @param dimensionSlice dimension values to filter by, {@code null} means any non-null value.
   * @param startTs start timestamp, in sec
   * @param endTs end timestamp, in sec
   * @return {@link Set} of measure names
   */
  // todo: pass a limit on number of measures returned
  public Set<String> findMeasureNames(List<String> allDimensionNames, Map<String, String> dimensionSlice,
                                      long startTs, long endTs) {

    List<DimensionValue> allDimensions = Lists.newArrayList();
    for (String dimensionName : allDimensionNames) {
      allDimensions.add(new DimensionValue(dimensionName, dimensionSlice.get(dimensionName)));
    }

    byte[] startRow = codec.createStartRowKey(allDimensions, null, startTs, false);
    byte[] endRow = codec.createEndRowKey(allDimensions, null, endTs, false);
    endRow = Bytes.stopKeyForPrefix(endRow);
    FuzzyRowFilter fuzzyRowFilter =
      createFuzzyRowFilter(new FactScan(startTs, endTs, ImmutableList.<String>of(), allDimensions), startRow);

    Set<String> measureNames = Sets.newHashSet();
    int scannedRecords = 0;
    // todo: make configurable

    try (Scanner scanner = timeSeriesTable.scan(startRow, endRow, fuzzyRowFilter)) {
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
    }

    LOG.trace("search for measures completed, scanned records: {}", scannedRecords);

    return measureNames;
  }

  @Override
  public void close() throws IOException {
    timeSeriesTable.close();
    entityTable.close();
  }

  public static byte[][] getSplits(int aggGroupsCount) {
    return FactCodec.getSplits(aggGroupsCount);
  }

  private FuzzyRowFilter createFuzzyRowFilter(FactScan scan, List<String> measureNames) {
    List<ImmutablePair<byte[], byte[]>> fuzzyPairsList = new ArrayList<>();
    for (String measureName : measureNames) {
      // add exact fuzzy keys for all the measure names provided in the scan, when constructing fuzzy row filter
      // its okay to use startTs as timebase part of rowKey is always fuzzy in fuzzy filter
      byte[] startRow = codec.createStartRowKey(scan.getDimensionValues(), measureName, scan.getStartTs(), false);
      byte[] fuzzyRowMask = codec.createFuzzyRowMask(scan.getDimensionValues(), measureName);
      fuzzyPairsList.add(new ImmutablePair<>(startRow, fuzzyRowMask));
    }
    return new FuzzyRowFilter(fuzzyPairsList);
  }

  private FuzzyRowFilter createFuzzyRowFilter(FactScan scan, byte[] startRow) {
    // we need to always use a fuzzy row filter as it is the only one to do the matching of values

    // if we are querying only one measure, we will use fixed measureName for filter,
    // if there are no measures or more than one measures to query we use `ANY` fuzzy filter.
    String measureName = (scan.getMeasureNames().size() == 1) ? scan.getMeasureNames().iterator().next() : null;
    byte[] fuzzyRowMask = codec.createFuzzyRowMask(scan.getDimensionValues(), measureName);
    // note: we can use startRow, as it will contain all "fixed" parts of the key needed
    return new FuzzyRowFilter(ImmutableList.of(new ImmutablePair<>(startRow, fuzzyRowMask)));
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
