/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.Scanner;
import com.continuuity.metrics.transport.MetricRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Table for storing metric.
 *
 * TODO: More doc.
 */
public final class MetricTable {

  private static final int MAX_ROLL_TIME = 0xfffe;
  private static final byte[] FOUR_ZERO_BYTES = {0, 0, 0, 0};

  private final OrderedVersionedColumnarTable metricTable;
  private final MetricEntityCodec entityCodec;
  private final boolean isFilterable;
  private final int resolution;
  private final int rollTimebaseInterval;
  private final ImmutablePair<byte[], byte[]> defaultTagFuzzyPair;

  // Cache for delta values.
  private final byte[][] deltaCache;

  /**
   * Creates a MetricTable. Same as calling
   * {@link #MetricTable(EntityTable, com.continuuity.data.table.OrderedVersionedColumnarTable,
   * int, int, int, int, int)}
   * with
   * <p>
   * {@code contextDepth = }{@link MetricConstants#DEFAULT_CONTEXT_DEPTH}
   * </p>
   * <p>
   * {@code metricDepth = }{@link MetricConstants#DEFAULT_METRIC_DEPTH}
   * </p>
   * <p>
   * {@code tagDepth = }{@link MetricConstants#DEFAULT_TAG_DEPTH}
   * </p>
   */
  public MetricTable(EntityTable entityTable, OrderedVersionedColumnarTable metricTable, int resolution, int rollTime) {
    this(entityTable, metricTable,
         MetricConstants.DEFAULT_CONTEXT_DEPTH,
         MetricConstants.DEFAULT_METRIC_DEPTH,
         MetricConstants.DEFAULT_TAG_DEPTH,
         resolution, rollTime);
  }

  /**
   * Creates a MetricTable.
   *
   * @param entityTable For lookup from entity name to uniqueID.
   * @param metricTable A OVC table for storing metric information.
   * @param contextDepth Maximum level in context
   * @param metricDepth Maximum level in metric
   * @param tagDepth Maximum level in tag
   * @param resolution Resolution in second of the table
   * @param rollTime Number of resolution for writing to a new row with a new timebase.
   *                 Meaning the differences between timebase of two consecutive rows divided by
   *                 resolution seconds. It essentially defines how many columns per row in the table.
   *                 This value should be < 65535.
   */
  public MetricTable(EntityTable entityTable, OrderedVersionedColumnarTable metricTable,
                     int contextDepth, int metricDepth, int tagDepth,
                     int resolution, int rollTime) {

    this.metricTable = metricTable;
    this.entityCodec = new MetricEntityCodec(entityTable, contextDepth, metricDepth, tagDepth);
    this.isFilterable = metricTable instanceof FilterableOVCTable;
    this.resolution = resolution;

    // Two bytes for column name, which is a delta timestamp
    Preconditions.checkArgument(rollTime <= MAX_ROLL_TIME, "Rolltime should be <= " + MAX_ROLL_TIME);
    this.rollTimebaseInterval = rollTime * resolution;
    this.deltaCache = createDeltaCache(rollTime);

    this.defaultTagFuzzyPair = createDefaultTagFuzzyPair();
  }

  /**
   * Saves a collection of {@link MetricRecord}.
   */
  public void save(Collection<MetricRecord> records) throws OperationException {
    // Simply collecting all rows/cols/values that need to be put to the underlying table.
    Table<byte[], byte[], byte[]> table = TreeBasedTable.create(Bytes.BYTES_COMPARATOR, Bytes.BYTES_COMPARATOR);

    for (MetricRecord record : records) {
      getUpdates(record, table);
    }

    // Covert the table into the format needed by the put method.
    Map<byte[], Map<byte[], byte[]>> rowMap = table.rowMap();
    byte[][] rows = new byte[rowMap.size()][];
    byte[][][] columns = new byte[rowMap.size()][][];
    byte[][][] values = new byte[rowMap.size()][][];

    int rowIdx = 0;
    for (Map.Entry<byte[], Map<byte[], byte[]>> rowEntry : rowMap.entrySet()) {
      rows[rowIdx] = rowEntry.getKey();
      Map<byte[], byte[]> colMap = rowEntry.getValue();
      columns[rowIdx] = new byte[colMap.size()][];
      values[rowIdx] = new byte[colMap.size()][];

      int colIdx = 0;
      for (Map.Entry<byte[], byte[]> colEntry : colMap.entrySet()) {
        columns[rowIdx][colIdx] = colEntry.getKey();
        values[rowIdx][colIdx] = colEntry.getValue();
        colIdx++;
      }
      rowIdx++;
    }

    metricTable.put(rows, columns, System.currentTimeMillis(), values);
  }

  /**
   * Query single metric.
   *
   * @param startTime Start timestamp in seconds (inclusive).
   * @param endTime End timestamp in seconds (inclusive).
   * @param context Complete context.
   * @param metricName Complete metric name.
   *
   * @return An Iterable of {@link TimeValue} which gives metric value associated with each timestamp data point
   *         in ascending order.
   */
  public Iterable<TimeValue> query(long startTime, long endTime,
                                   String context, String metricName) throws OperationException {
    return query(startTime, endTime, context, metricName, null);
  }

  /**
   * Query single metric.
   *
   * @param startTime Start timestamp in seconds (inclusive).
   * @param endTime End timestamp in seconds (inclusive).
   * @param context Complete context.
   * @param metricName Complete metric name.
   * @param tag Complete tag. If null, it is ignored.
   *
   * @return An Iterable of {@link TimeValue} which gives metric value associated with each timestamp data point
   *         in ascending order.
   */
  public Iterable<TimeValue> query(long startTime, long endTime,
                                   String context, String metricName, String tag) throws OperationException {
    final int startTimeBase = getTimeBase(startTime);
    int endTimeBase = getTimeBase(endTime);

    byte[] startRow = getKey(context, metricName, tag, startTimeBase);

    // Single row
    if (startTimeBase == endTimeBase) {
      int startDelta = (int) (startTime - startTimeBase);
      int endDelta = (int) (endTime - endTimeBase) + 1;
      OperationResult<Map<byte[], byte[]>> result = metricTable.get(startRow,
                                                                    deltaCache[startDelta], deltaCache[endDelta],
                                                                    endDelta - startDelta,
                                                                    MemoryReadPointer.DIRTY_READ);
      if (result.isEmpty()) {
        return ImmutableList.of();
      }
      return new TimeValueIterable(startTimeBase, resolution, startTime, endTime, result.getValue().entrySet());
    }

    // Multiple rows
    byte[] stopRow = getKey(context, metricName, tag, endTimeBase + 1);

    Scanner scanner = metricTable.scan(startRow, stopRow, deltaCache, MemoryReadPointer.DIRTY_READ);

    // Creates one TimeValueIterable per row and returns a concatenation of them.
    List<Iterable<TimeValue>> iterables = Lists.newLinkedList();

    ImmutablePair<byte[], Map<byte[], byte[]>> rowResult = scanner.next();
    while (rowResult != null) {
      byte[] rowKey = rowResult.getFirst();

      // Last 4 bytes is the timebase.
      int timeBase = Bytes.toInt(rowKey, rowKey.length - 4, 4);
      iterables.add(new TimeValueIterable(timeBase, resolution, startTime, endTime, rowResult.getSecond().entrySet()));

      rowResult = scanner.next();
    }
    scanner.close();

    return Iterables.concat(iterables);
  }


  public MetricScanner scan(MetricScanQuery query) {
    int startTimeBase = getTimeBase(query.getStartTime());
    int endTimeBase = getTimeBase(query.getEndTime());

    byte[] startRow = getPaddedKey(query.getContextPrefix(),
                                   query.getMetricPrefix(), query.getTagPrefix(), startTimeBase, 0);
    byte[] endRow = getPaddedKey(query.getContextPrefix(),
                                 query.getMetricPrefix(), query.getTagPrefix(), endTimeBase + 1, 0xff);

    Scanner scanner;
    if (isFilterable) {
      scanner = ((FilterableOVCTable) metricTable).scan(startRow, endRow,
                                                        MemoryReadPointer.DIRTY_READ,
                                                        getFilter(query, startTimeBase, endTimeBase));
    } else {
      scanner = metricTable.scan(startRow, endRow, MemoryReadPointer.DIRTY_READ);
    }
    return new MetricScanner(query, scanner, entityCodec, resolution);
  }

  /**
   * Setups all rows, columns and values for updating the metric table.
   */
  private void getUpdates(MetricRecord record, Table<byte[], byte[], byte[]> table) {
    long timestamp = record.getTimestamp() / resolution * resolution;
    int timeBase = getTimeBase(timestamp);

    // Key for the no tag one
    byte[] rowKey = getKey(record.getContext(), record.getName(), null, timeBase);

    // delta is guaranteed to be 2 bytes.
    byte[] column = deltaCache[(int) (timestamp - timeBase)];

    table.put(rowKey, column, Bytes.toBytes(record.getValue()));

    // Save the context, metric columns
    table.put(rowKey, MetricConstants.CONTEXT, entityCodec.encode(MetricEntityType.CONTEXT, record.getContext()));
    table.put(rowKey, MetricConstants.METRIC, entityCodec.encode(MetricEntityType.METRIC, record.getName()));

    for (TagMetric tag : record.getTags()) {
      rowKey = getKey(record.getContext(), record.getName(), tag.getTag(), timeBase);
      table.put(rowKey, column, Bytes.toBytes(tag.getValue()));

      // Save the context, metric columns
      table.put(rowKey, MetricConstants.CONTEXT, entityCodec.encode(MetricEntityType.CONTEXT, record.getContext()));
      table.put(rowKey, MetricConstants.METRIC, entityCodec.encode(MetricEntityType.METRIC, record.getName()));
    }
  }

  /**
   * Creates the row key for the given context, metric, tag, and timebase.
   */
  private byte[] getKey(String context, String metric, String tag, int timeBase) {
    Preconditions.checkArgument(context != null, "Context cannot be null.");
    Preconditions.checkArgument(metric != null, "Metric cannot be null.");

    return concatBytes(entityCodec.encode(MetricEntityType.CONTEXT, context),
                       entityCodec.encode(MetricEntityType.METRIC, metric),
                       entityCodec.encode(MetricEntityType.TAG, tag == null ? MetricConstants.EMPTY_TAG : tag),
                       Bytes.toBytes(timeBase));
  }

  private byte[] getPaddedKey(String contextPrefix, String metricPrefix, String tagPrefix, int timeBase, int padding) {

    Preconditions.checkArgument(contextPrefix != null, "Context cannot be null.");
    Preconditions.checkArgument(metricPrefix != null, "Metric cannot be null.");

    return concatBytes(
      entityCodec.paddedEncode(MetricEntityType.CONTEXT, contextPrefix, padding),
      entityCodec.paddedEncode(MetricEntityType.METRIC, metricPrefix, padding),
      entityCodec.paddedEncode(MetricEntityType.TAG,
                               tagPrefix == null ? MetricConstants.EMPTY_TAG : tagPrefix, padding),
      Bytes.toBytes(timeBase));
  }

  private Filter getFilter(MetricScanQuery query, long startTimeBase, long endTimeBase) {
    String tag = query.getTagPrefix();

    // Create fuzzy row filter
    ImmutablePair<byte[], byte[]> contextPair = entityCodec.paddedFuzzyEncode(MetricEntityType.CONTEXT,
                                                                              query.getContextPrefix(), 0);
    ImmutablePair<byte[], byte[]> metricPair = entityCodec.paddedFuzzyEncode(MetricEntityType.METRIC,
                                                                             query.getMetricPrefix(), 0);
    ImmutablePair<byte[], byte[]> tagPair = (tag == null) ? defaultTagFuzzyPair
                                                          : entityCodec.paddedFuzzyEncode(MetricEntityType.TAG, tag, 0);

    // For each timbase, construct a fuzzy filter pair
    List<Pair<byte[], byte[]>> fuzzyPairs = Lists.newLinkedList();
    for (long timeBase = startTimeBase; timeBase <= endTimeBase; timeBase += this.rollTimebaseInterval) {
      fuzzyPairs.add(Pair.newPair(concatBytes(contextPair.getFirst(), metricPair.getFirst(),
                                              tagPair.getFirst(), Bytes.toBytes((int) timeBase)),
                                  concatBytes(contextPair.getSecond(), metricPair.getSecond(),
                                              tagPair.getSecond(), FOUR_ZERO_BYTES)));
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

  private byte[] concatBytes(byte[]...array) {
    return com.google.common.primitives.Bytes.concat(array);
  }

  private ImmutablePair<byte[], byte[]> createDefaultTagFuzzyPair() {
    byte[] key = entityCodec.encode(MetricEntityType.TAG, MetricConstants.EMPTY_TAG);
    byte[] mask = new byte[key.length];
    Arrays.fill(mask, (byte) 0);
    return new ImmutablePair<byte[], byte[]>(key, mask);
  }
}

