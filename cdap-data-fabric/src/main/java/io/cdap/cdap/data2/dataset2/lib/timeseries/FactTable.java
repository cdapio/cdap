/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
package io.cdap.cdap.data2.dataset2.lib.timeseries;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.cube.DimensionValue;
import io.cdap.cdap.api.dataset.lib.cube.MeasureType;
import io.cdap.cdap.api.dataset.lib.cube.Measurement;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

  private final MetricsTable timeSeriesTable;
  private final EntityTable entityTable;
  private final FactCodec codec;
  private final int resolution;
  // todo: should not be used outside of codec
  private final int rollTime;

  private final String putCountMetric;
  private final String incrementCountMetric;
  private final Cache<FactCacheKey, Long> factCounterCache;

  @Nullable
  private MetricsCollector metrics;

  /**
   * Creates an instance of {@link FactTable}.
   * @param timeSeriesTable A table for storing facts information.
   * @param entityTable The table for storing dimension encoding mappings.
   * @param resolution Resolution in seconds
   * @param rollTime Number of resolution for writing to a new row with a new timebase.
*                 Meaning the differences between timebase of two consecutive rows divided by
*                 resolution seconds. It essentially defines how many columns per row in the table.
   * @param coarseLagFactor
   * @param coarseRoundFactor
   */
  public FactTable(MetricsTable timeSeriesTable,
                   EntityTable entityTable, int resolution, int rollTime, int coarseLagFactor, int coarseRoundFactor) {
    // Two bytes for column name, which is a delta timestamp
    Preconditions.checkArgument(rollTime <= MAX_ROLL_TIME, "Rolltime should be <= " + MAX_ROLL_TIME);

    this.entityTable = entityTable;
    this.timeSeriesTable = timeSeriesTable;
    this.codec = new FactCodec(entityTable, resolution, rollTime, coarseLagFactor, coarseRoundFactor);
    this.resolution = resolution;
    this.rollTime = rollTime;
    this.putCountMetric = "factTable." + resolution + ".put.count";
    this.incrementCountMetric = "factTable." + resolution + ".increment.count";

    // only use the cache if the resolution is not the total resolution
    this.factCounterCache = resolution == Integer.MAX_VALUE ? null :
      CacheBuilder.newBuilder().expireAfterAccess(1L, TimeUnit.MINUTES).maximumSize(100000).build();
  }

  public void setMetricsCollector(MetricsCollector metrics) {
    this.metrics = metrics;
  }

  public int add(List<Fact> facts) {
    // Simply collecting all rows/cols/values that need to be put to the underlying table.
    Map<FactMeasurementKey, Long> gaugesTable = new HashMap<>();
    Map<FactMeasurementKey, Long> incrementsTable = new HashMap<>();
    long nowSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    // this map is used to store metrics which was COUNTER type, but can be considered as GAUGE, which means it is
    // guaranteed to be a new row key in the underlying table.
    Map<FactMeasurementKey, Long> incGaugeTable = new HashMap<>();
    // this map is used to store the updated timestamp for the cache
    Map<FactCacheKey, Long> cacheUpdates = new HashMap<>();
    for (Fact fact : facts) {
      for (Measurement measurement : fact.getMeasurements()) {
        // round to the resolution timestamp
        long tsToResolution = codec.roundToResolution(fact.getTimestamp(), nowSeconds);
        FactMeasurementKey key = new FactMeasurementKey(tsToResolution,
                                                        fact.getDimensionValues(),
                                                        measurement.getName());
        if (MeasureType.COUNTER == measurement.getType()) {
          if (factCounterCache != null) {
            FactCacheKey cacheKey = new FactCacheKey(fact.getDimensionValues(), measurement.getName());
            Long existingTs = factCounterCache.getIfPresent(cacheKey);

            // if there is no existing ts or existing ts is greater than or equal to the current ts, this metric value
            // cannot be considered as a gauge, and we should update the incrementsTable
            if (existingTs == null || existingTs >= tsToResolution) {
              inc(incrementsTable, key, measurement.getValue());
              // if the current ts is greater than existing ts, then we can consider this metric as a newly seen metric
              // and perform gauge on this metric
            } else {
              inc(incGaugeTable, key, measurement.getValue());
            }

            // if there is no existing value or the current ts is greater than the existing ts, the value in the cache
            // should be updated
            if (existingTs == null || existingTs < tsToResolution) {
              cacheUpdates.compute(
                cacheKey, (k, oldValue) -> oldValue == null || tsToResolution > oldValue ? tsToResolution : oldValue);
            }
          } else {
            inc(incrementsTable, key, measurement.getValue());
          }
        } else {
          gaugesTable
            .put(key, measurement.getValue());
        }
      }
    }

    if (factCounterCache != null) {
      gaugesTable.putAll(incGaugeTable);
      factCounterCache.putAll(cacheUpdates);
    }
    // We use HashMap as a fast L0 cache that we create and throw out after each batch
    Map<EntityTable.EntityName, Long> cache = new HashMap<>();
    BiFunction<EntityTable.EntityName, Supplier<Long>, Long> cacheFunction = (name, loader) ->
      cache.computeIfAbsent(name, nm -> loader.get());
    // todo: replace with single call, to be able to optimize rpcs in underlying table
    timeSeriesTable.put(toColumnarFormat(gaugesTable, nowSeconds, cacheFunction));
    timeSeriesTable.increment(toColumnarFormat(incrementsTable, nowSeconds, cacheFunction));
    if (metrics != null) {
      metrics.increment(putCountMetric, gaugesTable.size());
      metrics.increment(incrementCountMetric, incrementsTable.size());
    }
    return gaugesTable.size() + incrementsTable.size();
  }

  private NavigableMap<byte[], NavigableMap<byte[], Long>> toColumnarFormat(
    Map<FactMeasurementKey, Long> data, long nowSeconds,
    BiFunction<EntityTable.EntityName, Supplier<Long>, Long> fastCache) {

    return data.entrySet().stream().collect(Collectors.groupingBy(
      entry -> codec.createRowKey(entry.getKey().dimensionValues, entry.getKey().measurementName,
                                  entry.getKey().timestamp, nowSeconds, fastCache),
      () -> Maps.newTreeMap(Bytes.BYTES_COMPARATOR),
      Collectors.toMap(
        entry -> codec.createColumn(entry.getKey().timestamp, nowSeconds),
        entry -> entry.getValue(),
        (u, v) -> {
          throw new IllegalStateException(String.format("Duplicate key %s", u));
          },
        () -> Maps.newTreeMap(Bytes.BYTES_COMPARATOR)
      )
    ));
  }

  private class FactMeasurementKey {
    private final long timestamp;
    private final List<DimensionValue> dimensionValues;
    private final String measurementName;

    private FactMeasurementKey(long timestamp, List<DimensionValue> dimensionValues, String measurementName) {
      this.timestamp = timestamp;
      this.dimensionValues = dimensionValues;
      this.measurementName = measurementName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FactMeasurementKey that = (FactMeasurementKey) o;
      return timestamp == that.timestamp
        && dimensionValues.equals(that.dimensionValues)
        && measurementName.equals(that.measurementName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, dimensionValues, measurementName);
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
    measureNames.sort(new MeasureNameComparator(measureToEntityMap));
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
                timeSeriesTable, scan, Bytes.toHexString(startRow),
                endRow == null ? null : Bytes.toHexString(endRow), fuzzyRowFilter);
    }

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
        boolean fullRow = true;
        for (byte[] column : row.getColumns().keySet()) {
          long ts = codec.getTimestamp(row.getRow(), column);
          if (ts < scan.getStartTs()) {
            fullRow = false;
            continue;
          }

          if (ts > scan.getEndTs()) {
            exhausted = true;
            fullRow = false;
            break;
          }

          columns.add(column);
        }

        // todo: do deletes efficiently, in batches, not one-by-one
        timeSeriesTable.delete(row.getRow(), columns.toArray(new byte[columns.size()][]), fullRow);

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
    List<Integer> dimToFillIndexes = Lists.newArrayList();
    for (int i = 0; i < allDimensionNames.size(); i++) {
      String dimensionName = allDimensionNames.get(i);
      if (!dimensionSlice.containsKey(dimensionName)) {
        dimToFillIndexes.add(i);
        allDimensions.add(new DimensionValue(dimensionName, null));
      } else {
        DimensionValue dimensionValue = new DimensionValue(dimensionName, dimensionSlice.get(dimensionName));
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
      createFuzzyRowFilter(new FactScan(startTs, endTs, Collections.emptyList(), allDimensions), startRow);
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
        if (codec.getTimestamp(rowKey, codec.createColumn(startTs, startTs)) < startTs) {
          continue;
        }
        if (codec.getTimestamp(rowKey, codec.createColumn(endTs, endTs)) > endTs) {
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
      createFuzzyRowFilter(new FactScan(startTs, endTs, Collections.emptyList(), allDimensions), startRow);

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
        if (codec.getTimestamp(rowKey, codec.createColumn(startTs, startTs)) < startTs) {
          continue;
        }
        if (codec.getTimestamp(rowKey, codec.createColumn(endTs, endTs)) > endTs) {
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

  @VisibleForTesting
  Cache<FactCacheKey, Long> getFactCounterCache() {
    return factCounterCache;
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
  private static void inc(Map<FactMeasurementKey, Long> incrementsTable,
                          FactMeasurementKey key, long value) {
    incrementsTable.compute(key, (k, prev) -> value + (prev == null ? 0 : prev));
  }

  class FactCacheKey {
    private final List<DimensionValue> dimensionValues;
    private final String metricName;

    FactCacheKey(List<DimensionValue> dimensionValues, String metricName) {
      this.dimensionValues = dimensionValues;
      this.metricName = metricName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FactCacheKey that = (FactCacheKey) o;
      return Objects.equals(dimensionValues, that.dimensionValues) &&
        Objects.equals(metricName, that.metricName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dimensionValues, metricName);
    }
  }
}
