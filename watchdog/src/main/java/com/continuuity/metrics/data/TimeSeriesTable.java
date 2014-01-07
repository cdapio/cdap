/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.dataset.lib.table.FuzzyRowFilter;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
   * Saves a collection of {@link com.continuuity.metrics.transport.MetricsRecord}.
   */
  public void save(Iterable<MetricsRecord> records) throws OperationException {
    save(records.iterator());
  }

  public void save(Iterator<MetricsRecord> records) throws OperationException {
    if (!records.hasNext()) {
      return;
    }

    // Simply collecting all rows/cols/values that need to be put to the underlying table.
    Table<byte[], byte[], byte[]> table = TreeBasedTable.create(Bytes.BYTES_COMPARATOR, Bytes.BYTES_COMPARATOR);

    while (records.hasNext()) {
      getUpdates(records.next(), table);
    }

    try {
      timeSeriesTable.put(table.rowMap());
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  public MetricsScanner scan(MetricsScanQuery query) throws OperationException {
    return scanFor(query, false);
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
      ImmutablePair<byte[], Map<byte[], byte[]>> nextEntry;
      while ((nextEntry = scanner.next()) != null) {
        byte[] rowKey = nextEntry.getFirst();

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
  private void getUpdates(MetricsRecord record, Table<byte[], byte[], byte[]> table) {
    long timestamp = record.getTimestamp() / resolution * resolution;
    int timeBase = getTimeBase(timestamp);

    // Key for the no tag one
    byte[] rowKey = getKey(record.getContext(), record.getRunId(), record.getName(), null, timeBase);

    // delta is guaranteed to be 2 bytes.
    byte[] column = deltaCache[(int) (timestamp - timeBase)];

    addValue(rowKey, column, table, record.getValue());

    // Save tags metrics
    for (TagMetric tag : record.getTags()) {
      rowKey = getKey(record.getContext(), record.getRunId(), record.getName(), tag.getTag(), timeBase);
      addValue(rowKey, column, table, tag.getValue());
    }
  }

  private void addValue(byte[] rowKey, byte[] column, Table<byte[], byte[], byte[]> table, int value) {
    byte[] oldValue = table.get(rowKey, column);
    int newValue = value;
    if (oldValue != null) {
      newValue = Bytes.toInt(oldValue) + value;
    }
    table.put(rowKey, column, Bytes.toBytes(newValue));
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

  private FuzzyRowFilter getFilter(MetricsScanQuery query, long startTimeBase,
                                   long endTimeBase, boolean shouldMatchAllTags) {
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
                                                   FOUR_ZERO_BYTES, runIdPair.getSecond())));
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
    FuzzyRowFilter filter = getFilter(query, startTimeBase, endTimeBase, shouldMatchAllTags);

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

