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
package co.cask.cdap.metrics.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.data2.StatusCode;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.metrics.transport.TagMetric;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

/**
 * Table for storing time series metrics.
 * <p>
 * Row key:
 * {@code context|metricName|tags|timebase|runId}
 * </p>
 * <p>
 * Columns: offset to timebase of the row.
 * </p>
 * <p>
 * Cell: Value for the metrics specified by the row with at the timestamp of (timbase + offset) * resolution.
 * </p>
 * <p>
 * TODO: More doc.
 * </p>
 */
public final class TimeSeriesTable {

  private static final int MAX_ROLL_TIME = 0xfffe;
  private static final byte[] FOUR_ZERO_BYTES = {0, 0, 0, 0};
  private static final byte[] FOUR_ONE_BYTES = {1, 1, 1, 1};
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
  private final MetricsEntityCodec entityCodec;
  private final int resolution;
  private final int rollTimebaseInterval;
  private final ImmutablePair<byte[], byte[]> defaultTagFuzzyPair;

  // Cache for delta values.
  private final byte[][] deltaCache;

  /**
   * Creates a MetricTable.
   *
   * @param timeSeriesTable A OVC table for storing metric information.
   * @param entityCodec The {@link MetricsEntityCodec} for encoding entity.
   * @param resolution Resolution in second of the table
   * @param rollTime Number of resolution for writing to a new row with a new timebase.
   *                 Meaning the differences between timebase of two consecutive rows divided by
   *                 resolution seconds. It essentially defines how many columns per row in the table.
   *                 This value should be < 65535.
   */
  TimeSeriesTable(MetricsTable timeSeriesTable,
                  MetricsEntityCodec entityCodec, int resolution, int rollTime) {

    this.timeSeriesTable = timeSeriesTable;
    this.entityCodec = entityCodec;
    this.resolution = resolution;
    // Two bytes for column name, which is a delta timestamp
    Preconditions.checkArgument(rollTime <= MAX_ROLL_TIME, "Rolltime should be <= " + MAX_ROLL_TIME);
    this.rollTimebaseInterval = rollTime * resolution;
    this.deltaCache = createDeltaCache(rollTime);

    this.defaultTagFuzzyPair = createDefaultTagFuzzyPair();
  }

  /**
   * Saves a collection of {@link co.cask.cdap.metrics.transport.MetricsRecord}. If the
   * {@link co.cask.cdap.metrics.transport.MetricType} is Counter, we would perform an increment and if its of
   * Gauge type we perform a put operation
   */
  public void save(Iterable<MetricsRecord> records) throws OperationException {
    save(records.iterator());
  }

  public void save(Iterator<MetricsRecord> records) throws OperationException {
    if (!records.hasNext()) {
      return;
    }

    // Simply collecting all rows/cols/values that need to be put to the underlying table.
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> gaugesTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> incrementsTable = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);


    while (records.hasNext()) {
      getUpdates(records.next(), gaugesTable, incrementsTable);
    }

    NavigableMap<byte[], NavigableMap<byte[], Long>> convertedIncrementsTable =
      Maps.transformValues(incrementsTable, TRANSFORM_MAP_BYTE_ARRAY_TO_LONG);

    NavigableMap<byte[], NavigableMap<byte[], Long>> convertedGaugesTable =
      Maps.transformValues(gaugesTable, TRANSFORM_MAP_BYTE_ARRAY_TO_LONG);

    try {
      timeSeriesTable.put(convertedGaugesTable);
      timeSeriesTable.increment(convertedIncrementsTable);
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  public MetricsScanner scan(MetricsScanQuery query) throws OperationException {
    return scanFor(query, false);
  }

  public List<String> getNextLevelContexts(MetricsScanQuery query) throws OperationException {
    return getAvailableContextAndMetrics(query, true);
  }

  public List<String> getAllMetrics(MetricsScanQuery query) throws OperationException {
    return getAvailableContextAndMetrics(query, false);
  }

  private List<String> getAvailableContextAndMetrics(MetricsScanQuery query, boolean isContextQuery)
    throws OperationException {
    List<String> metricsScanResults = Lists.newArrayList();
    int startTimeBase = getTimeBase(query.getStartTime());
    int endTimeBase = getTimeBase(query.getEndTime());

    byte[] contextStart = null, contextEnd = null, metricsPrefix = null;
    byte[] startRow = null, endRow = null;
    byte[][] columns = null;
    String tagPrefix = query.getTagPrefix();
    if (tagPrefix == null) {
      tagPrefix = MetricsConstants.EMPTY_TAG;
    }
    if (isContextQuery) {
      contextStart = entityCodec.paddedEncode(MetricsEntityType.CONTEXT, query.getContextPrefix(), 0);
      contextEnd = entityCodec.paddedEncode(MetricsEntityType.CONTEXT, query.getContextPrefix(), 0xff);
    } else {
      metricsPrefix = entityCodec.paddedEncode(MetricsEntityType.METRIC, query.getMetricPrefix(), 0);
    }

    Row rowResult;
    do {
      if (isContextQuery) {
        startRow = getNextPaddedKey(contextStart, query.getRunId(),
                                    query.getMetricPrefix(), tagPrefix, startTimeBase, 0);
        endRow = getNextPaddedKey(contextEnd, query.getRunId(),
                                  query.getMetricPrefix(), tagPrefix, endTimeBase + 1, 0xff);
      } else {
        startRow = getNextPaddedKey(query.getContextPrefix(), query.getRunId(),
                                    metricsPrefix, tagPrefix, startTimeBase, 0);
        endRow = getNextPaddedKey(query.getContextPrefix(), query.getRunId(),
                                  metricsPrefix, tagPrefix, endTimeBase + 1, 0xff);
      }

      FuzzyRowFilter filter = getFilter(query, startTimeBase, endTimeBase + 1, false, FOUR_ONE_BYTES);

      ScannerFields fields = new ScannerFields(startRow, endRow, columns, filter);
      Scanner scanner = null;
      try {
        scanner = timeSeriesTable.scan(fields.startRow, fields.endRow, fields.columns, fields.filter);
      } catch (Exception e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
      }

      rowResult = scanner.next();
      if (rowResult != null) {
        byte[] rowKey = rowResult.getRow();
        // Decode context and metric from key
        int offset = 0;
        String contextStr = entityCodec.decode(MetricsEntityType.CONTEXT, rowKey, offset);

        // Always have a "." suffix for unique matching
        if (query.getContextPrefix() != null && !(contextStr + ".").startsWith(query.getContextPrefix())) {
          scanner.close();
          break;
        }

        offset += entityCodec.getEncodedSize(MetricsEntityType.CONTEXT);
        String metricName = entityCodec.decode(MetricsEntityType.METRIC, rowKey, offset);

        if (isContextQuery) {
          contextStart = getNextContextTarget(contextStart, query.getContextPrefix());
          metricsScanResults.add(contextStr);
        } else {
          metricsPrefix = getNextMetricsTarget(
            Arrays.copyOfRange(rowKey, entityCodec.getEncodedSize(MetricsEntityType.CONTEXT),
                               entityCodec.getEncodedSize(MetricsEntityType.CONTEXT) +
                                 entityCodec.getEncodedSize(MetricsEntityType.METRIC)));
          metricsScanResults.add(metricName);
        }
      }
      scanner.close();
    } while (rowResult != null);

    return metricsScanResults;
  }

  private byte[] getNextMetricsTarget(byte[] metricsArray) {
    int idSize = entityCodec.getIdSize();
    String metricsString = entityCodec.decode(MetricsEntityType.METRIC, metricsArray);
    int targetContextLocation = StringUtils.countMatches(metricsString, ".") + 1;
    byte[] targetContextIncrement = Bytes.stopKeyForPrefix(
      Arrays.copyOfRange(metricsArray, idSize * targetContextLocation, (idSize * targetContextLocation + idSize)));
    return Bytes.concat(Bytes.head(metricsArray, idSize * targetContextLocation), targetContextIncrement,
                        Bytes.tail(metricsArray, entityCodec.getEncodedSize(
                          MetricsEntityType.METRIC) - (idSize + 1) * targetContextLocation));
  }


  private byte[] getNextContextTarget(byte[] context, String contextPrefix) {
    int idSize = entityCodec.getIdSize();
    int targetContextLocation = contextPrefix == null ? 0 : StringUtils.countMatches(contextPrefix, ".") + 1;
    byte[] targetContextIncrement = Bytes.stopKeyForPrefix(
      Arrays.copyOfRange(context, idSize * targetContextLocation, (idSize * targetContextLocation + idSize)));
    return Bytes.concat(Bytes.head(context, idSize * targetContextLocation), targetContextIncrement,
                        Bytes.tail(context, entityCodec.getEncodedSize(
                          MetricsEntityType.CONTEXT) - (idSize + 1) * targetContextLocation));
  }

  public MetricsScanner scanAllTags(MetricsScanQuery query) throws OperationException {
    return scanFor(query, true);
  }

  /**
   * Deletes all the row keys which match the context prefix.
   *
   * @param contextPrefix Prefix of the context to match.  Must not be null, as full table deletes should be done
   *                      through the clear method.
   * @throws OperationException if there is an error in deleting entries.
   */
  public void delete(String contextPrefix) throws OperationException {
    Preconditions.checkArgument(contextPrefix != null, "null context not allowed for delete");
    try {
      timeSeriesTable.deleteAll(entityCodec.encodeWithoutPadding(MetricsEntityType.CONTEXT, contextPrefix));
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Deletes all the row keys which match the context prefix and metric prefix.  Context and Metric cannot both be
   * null, as full table deletes should be done through the clear method.
   *
   * @param contextPrefix Prefix of the context to match, null means any context.
   * @param metricPrefix Prefix of the metric to match, null means any metric.
   * @throws OperationException if there is an error in deleting entries.
   */
  public void delete(String contextPrefix, String metricPrefix) throws OperationException {
    Preconditions.checkArgument(contextPrefix != null || metricPrefix != null,
                                "context and metric cannot both be null");
    if (metricPrefix == null) {
      delete(contextPrefix);
    } else {
      byte[] startRow = getPaddedKey(contextPrefix, "0", metricPrefix, null, 0, 0);
      byte[] endRow = getPaddedKey(contextPrefix, "0", metricPrefix, null, Integer.MAX_VALUE, 0xff);
      try {
        // Create fuzzy row filter
        ImmutablePair<byte[], byte[]> contextPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.CONTEXT,
                                                                                  contextPrefix, 0);
        ImmutablePair<byte[], byte[]> metricPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.METRIC,
                                                                                 metricPrefix, 0);
        ImmutablePair<byte[], byte[]> tagPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.TAG, null, 0);
        ImmutablePair<byte[], byte[]> runIdPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.RUN, null, 0);
        FuzzyRowFilter filter = new FuzzyRowFilter(ImmutableList.of(ImmutablePair.of(
          Bytes.concat(contextPair.getFirst(), metricPair.getFirst(), tagPair.getFirst(),
                       Bytes.toBytes(0), runIdPair.getFirst()),
          Bytes.concat(contextPair.getSecond(), metricPair.getSecond(), tagPair.getSecond(),
                       FOUR_ONE_BYTES, runIdPair.getSecond()))));

        timeSeriesTable.deleteRange(startRow, endRow, null, filter);
      } catch (Exception e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
      }
    }
  }

  /**
   * Delete all entries that would match the given scan query.
   *
   * @param query Query specifying context, metric, runid, tag, and time range of entries to delete.  A null value for
   *              context, metric, and runId will match any value for those fields.  A null value for tag will
   *              match untagged entries, which is the same as using MetricsConstants.EMPTY_TAG.
   * @throws OperationException
   */
  public void delete(MetricsScanQuery query) throws OperationException {
    try {
      ScannerFields fields = getScannerFields(query);
      timeSeriesTable.deleteRange(fields.startRow, fields.endRow, fields.columns, fields.filter);
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Deletes all row keys that has timestamp before the given time.
   * @param beforeTime All data before this timestamp will be removed (exclusive).
   */
  public void deleteBefore(long beforeTime) throws OperationException {
    // End time base is the last time base that is smaller than endTime.
    int endTimeBase = getTimeBase(beforeTime);

    Scanner scanner = null;
    try {
      scanner = timeSeriesTable.scan(null, null, null, null);

      // Loop through the scanner entries and collect rows to be deleted
      List<byte[]> rows = Lists.newArrayList();
      Row nextEntry;
      while ((nextEntry = scanner.next()) != null) {
        byte[] rowKey = nextEntry.getRow();

        // Decode timestamp
        int offset = entityCodec.getEncodedSize(MetricsEntityType.CONTEXT) +
          entityCodec.getEncodedSize(MetricsEntityType.METRIC) +
          entityCodec.getEncodedSize(MetricsEntityType.TAG);
        int timeBase = Bytes.toInt(rowKey, offset, 4);
        if (timeBase < endTimeBase) {
          rows.add(rowKey);
        }
      }
      // If there is any row collected, delete them
      if (!rows.isEmpty()) {
        timeSeriesTable.delete(rows);
      }
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }


  /**
   * Clears the storage table.
   * @throws OperationException If error in clearing data.
   */
  public void clear() throws OperationException {
    try {
      timeSeriesTable.deleteAll(new byte[]{});
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  private MetricsScanner scanFor(MetricsScanQuery query, boolean shouldMatchAllTags) throws OperationException {
    try {
      ScannerFields fields = getScannerFields(query, shouldMatchAllTags);
      Scanner scanner = timeSeriesTable.scan(fields.startRow, fields.endRow, fields.columns, fields.filter);
      return new MetricsScanner(query, scanner, entityCodec, resolution);
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Setups all rows, columns and values for updating the metric table.
   */
  private void getUpdates(MetricsRecord record, NavigableMap<byte[], NavigableMap<byte[], byte[]>> gaugesTable,
                          NavigableMap<byte[], NavigableMap<byte[], byte[]>> incrementsTable) {
    long timestamp = record.getTimestamp() / resolution * resolution;
    int timeBase = getTimeBase(timestamp);

    // Key for the no tag one
    byte[] rowKey = getKey(record.getContext(), record.getRunId(), record.getName(), null, timeBase);

    // delta is guaranteed to be 2 bytes.
    byte[] column = deltaCache[(int) ((timestamp - timeBase) / resolution)];

    if (record.getType() == MetricType.COUNTER) {
      addValue(rowKey, column, incrementsTable, record.getValue());
    } else {
      put(gaugesTable, rowKey, column, Bytes.toBytes(record.getValue()));
    }

    // Save tags metrics
    for (TagMetric tag : record.getTags()) {
      rowKey = getKey(record.getContext(), record.getRunId(), record.getName(), tag.getTag(), timeBase);
      if (record.getType() == MetricType.COUNTER) {
        addValue(rowKey, column, incrementsTable, tag.getValue());
      } else {
        put(gaugesTable, rowKey, column, Bytes.toBytes(tag.getValue()));
      }
    }
  }

  private void addValue(byte[] rowKey, byte[] column,
                        NavigableMap<byte[], NavigableMap<byte[], byte[]>> incrementsTable,
                        long value) {
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
          String.format("Could not parse metric @row %s @column %s value %s as int or long",
                        Bytes.toStringBinary(rowKey), Bytes.toStringBinary(column), Bytes.toStringBinary(oldValue)));
      }

    }
    put(incrementsTable, rowKey, column, Bytes.toBytes(newValue));
  }

  private static byte[] get(NavigableMap<byte[], NavigableMap<byte[], byte[]>> table, byte[] row, byte[] column) {
    NavigableMap<byte[], byte[]> rowMap = table.get(row);
    return rowMap == null ? null : rowMap.get(column);
  }

  private static void put(NavigableMap<byte[], NavigableMap<byte[], byte[]>> table,
                          byte[] row, byte[] column, byte[] value) {
    NavigableMap<byte[], byte[]> rowMap = table.get(row);
    if (rowMap == null) {
      rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      table.put(row, rowMap);
    }

    rowMap.put(column, value);
  }

  /**
   * Creates the row key for the given context, metric, tag, and timebase.
   */
  private byte[] getKey(String context, String runId, String metric, String tag, int timeBase) {
    Preconditions.checkArgument(context != null, "Context cannot be null.");
    Preconditions.checkArgument(runId != null, "RunId cannot be null.");
    Preconditions.checkArgument(metric != null, "Metric cannot be null.");

    return Bytes.concat(entityCodec.encode(MetricsEntityType.CONTEXT, context),
                        entityCodec.encode(MetricsEntityType.METRIC, metric),
                        entityCodec.encode(MetricsEntityType.TAG, tag == null ? MetricsConstants.EMPTY_TAG : tag),
                        Bytes.toBytes(timeBase),
                        entityCodec.encode(MetricsEntityType.RUN, runId));
  }

  private byte[] getPaddedKey(String contextPrefix, String runId, String metricPrefix, String tagPrefix,
                              int timeBase, int padding) {

    // If there is no contextPrefix, metricPrefix or runId, just applies the padding
    return Bytes.concat(
      entityCodec.paddedEncode(MetricsEntityType.CONTEXT, contextPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.METRIC, metricPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.TAG, tagPrefix, padding),
      Bytes.toBytes(timeBase),
      entityCodec.paddedEncode(MetricsEntityType.RUN, runId, padding));
  }

  private byte[] getNextPaddedKey(byte[] contextPrefix, String runId, String metricPrefix, String tagPrefix,
                                  int timeBase, int padding) {

    // If there is no contextPrefix, metricPrefix or runId, just applies the padding
    return Bytes.concat(
      contextPrefix,
      entityCodec.paddedEncode(MetricsEntityType.METRIC, metricPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.TAG, tagPrefix, padding),
      Bytes.toBytes(timeBase),
      entityCodec.paddedEncode(MetricsEntityType.RUN, runId, padding));
  }

  private byte[] getNextPaddedKey(String contextPrefix, String runId, byte[] metricPrefix, String tagPrefix,
                                  int timeBase, int padding) {

    // If there is no contextPrefix, metricPrefix or runId, just applies the padding
    return Bytes.concat(
      entityCodec.paddedEncode(MetricsEntityType.CONTEXT, contextPrefix, padding),
      metricPrefix,
      entityCodec.paddedEncode(MetricsEntityType.TAG, tagPrefix, padding),
      Bytes.toBytes(timeBase),
      entityCodec.paddedEncode(MetricsEntityType.RUN, runId, padding));
  }

  private FuzzyRowFilter getFilter(MetricsScanQuery query, long startTimeBase,
                                   long endTimeBase, boolean shouldMatchAllTags, byte[] timeBaseMatch) {
    String tag = query.getTagPrefix();

    // Create fuzzy row filter
    ImmutablePair<byte[], byte[]> contextPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.CONTEXT,
                                                                              query.getContextPrefix(), 0);
    ImmutablePair<byte[], byte[]> metricPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.METRIC,
                                                                             query.getMetricPrefix(), 0);
    ImmutablePair<byte[], byte[]> tagPair = (!shouldMatchAllTags && tag == null)
                                                ? defaultTagFuzzyPair
                                                : entityCodec.paddedFuzzyEncode(MetricsEntityType.TAG, tag, 0);
    ImmutablePair<byte[], byte[]> runIdPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.RUN, query.getRunId(), 0);

    // For each timbase, construct a fuzzy filter pair
    List<ImmutablePair<byte[], byte[]>> fuzzyPairs = Lists.newLinkedList();
    for (long timeBase = startTimeBase; timeBase <= endTimeBase; timeBase += this.rollTimebaseInterval) {
      fuzzyPairs.add(ImmutablePair.of(Bytes.concat(contextPair.getFirst(), metricPair.getFirst(), tagPair.getFirst(),
                                                   Bytes.toBytes((int) timeBase), runIdPair.getFirst()),
                                      Bytes.concat(contextPair.getSecond(), metricPair.getSecond(), tagPair.getSecond(),
                                                   timeBaseMatch, runIdPair.getSecond())));
    }

    return new FuzzyRowFilter(fuzzyPairs);
  }

  /**
   * Returns timebase computed with the table setting for the given timestamp.
   */
  private int getTimeBase(long time) {
    // We are using 4 bytes timebase for row
    long timeBase = time / rollTimebaseInterval * rollTimebaseInterval;
    Preconditions.checkArgument(timeBase < 0x100000000L, "Timestamp is too large.");
    return (int) timeBase;
  }


  private byte[][] createDeltaCache(int rollTime) {
    byte[][] deltas = new byte[rollTime + 1][];

    for (int i = 0; i <= rollTime; i++) {
      deltas[i] = Bytes.toBytes((short) i);
    }
    return deltas;
  }

  private ImmutablePair<byte[], byte[]> createDefaultTagFuzzyPair() {
    byte[] key = entityCodec.encode(MetricsEntityType.TAG, MetricsConstants.EMPTY_TAG);
    byte[] mask = new byte[key.length];
    Arrays.fill(mask, (byte) 0);
    return new ImmutablePair<byte[], byte[]>(key, mask);
  }

  private  ScannerFields getScannerFields(MetricsScanQuery query) {
    return getScannerFields(query, false);
  }

  private ScannerFields getScannerFields(MetricsScanQuery query, boolean shouldMatchAllTags) {
    int startTimeBase = getTimeBase(query.getStartTime());
    int endTimeBase = getTimeBase(query.getEndTime());

    byte[][] columns = null;
    if (startTimeBase == endTimeBase) {
      // If on the same timebase, we only need subset of columns
      int startCol = (int) (query.getStartTime() - startTimeBase) / resolution;
      int endCol = (int) (query.getEndTime() - endTimeBase) / resolution;
      columns = new byte[endCol - startCol + 1][];

      for (int i = 0; i < columns.length; i++) {
        columns[i] = Bytes.toBytes((short) (startCol + i));
      }
    }

    String tagPrefix = query.getTagPrefix();
    if (!shouldMatchAllTags && tagPrefix == null) {
      tagPrefix = MetricsConstants.EMPTY_TAG;
    }
    byte[] startRow = getPaddedKey(query.getContextPrefix(), query.getRunId(),
                                   query.getMetricPrefix(), tagPrefix, startTimeBase, 0);
    byte[] endRow = getPaddedKey(query.getContextPrefix(), query.getRunId(),
                                 query.getMetricPrefix(), tagPrefix, endTimeBase + 1, 0xff);
    FuzzyRowFilter filter = getFilter(query, startTimeBase, endTimeBase, shouldMatchAllTags, FOUR_ZERO_BYTES);

    return new ScannerFields(startRow, endRow, columns, filter);
  }

  private class ScannerFields {
    private final byte[] startRow;
    private final byte[] endRow;
    private final byte[][] columns;
    private final FuzzyRowFilter filter;

    ScannerFields(byte[] startRow, byte[] endRow, byte[][] columns, FuzzyRowFilter filter) {
      this.startRow = startRow;
      this.endRow = endRow;
      this.columns = columns;
      this.filter = filter;
    }
  }
}
