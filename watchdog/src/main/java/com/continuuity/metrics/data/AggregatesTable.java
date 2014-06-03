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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;

/**
 * Table for storing aggregated metrics for all time.
 *
 * <p>
 *   Row key:
 *   {@code context|metric|runId}
 * </p>
 * <p>
 *   Column:
 *   {@code tag}
 * </p>
 */
public final class AggregatesTable {

  private final MetricsEntityCodec entityCodec;
  private final MetricsTable aggregatesTable;

  AggregatesTable(MetricsTable aggregatesTable, MetricsEntityCodec entityCodec) {
    this.entityCodec = entityCodec;
    this.aggregatesTable = aggregatesTable;
  }

  /**
   * Atomically compare a single metric entry with an expected value, and if it matches, replace it with a new value.
   *
   * @param context the context of the metric.
   * @param metric the name of the metric.
   * @param runId the runid of the metric.
   * @param tag the tag of the metric.  If null, the empty tag is used.
   * @param oldValue the expected value of the column. If null, this means that the column must not exist.
   * @param newValue the new value of the column. If null, the effect to delete the column if the comparison succeeds.
   * @return whether the write happened, that is, whether the existing value of the column matched the expected value.
   */
  public boolean swap(String context, String metric, String runId, String tag, Long oldValue, Long newValue)
    throws OperationException {
    byte[] row = getKey(context, metric, runId);
    byte[] col = (tag == null) ? Bytes.toBytes(MetricsConstants.EMPTY_TAG) : Bytes.toBytes(tag);
    try {
      byte[] oldVal = (oldValue == null) ? null : Bytes.toBytes(oldValue);
      byte[] newVal = (newValue == null) ? null : Bytes.toBytes(newValue);

      return aggregatesTable.swap(row, col, oldVal, newVal);
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Updates aggregates for the given list of {@link MetricsRecord}.
   *
   * @throws OperationException When there is an error updating the table.
   */
  public void update(Iterable<MetricsRecord> records) throws OperationException {
    update(records.iterator());
  }

  /**
   * Updates aggregates for the given iterator of {@link MetricsRecord}.
   *
   * @throws OperationException When there is an error updating the table.
   */
  public void update(Iterator<MetricsRecord> records) throws OperationException {
    try {
      while (records.hasNext()) {
        MetricsRecord record = records.next();
        byte[] rowKey = getKey(record.getContext(), record.getName(), record.getRunId());
        Map<byte[], Long> increments = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

        // The no tag value
        increments.put(Bytes.toBytes(MetricsConstants.EMPTY_TAG), (long) record.getValue());

        // For each tag, increments corresponding values
        for (TagMetric tag : record.getTags()) {
          increments.put(Bytes.toBytes(tag.getTag()), (long) tag.getValue());
        }
        aggregatesTable.increment(rowKey, increments);
      }
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Deletes all the row keys which match the context prefix.
   * @param contextPrefix Prefix of the context to match.  Null not allowed, full table deletes should be done through
   *                      the clear method.
   * @throws OperationException if there is an error in deleting entries.
   */
  public void delete(String contextPrefix) throws OperationException {
    Preconditions.checkArgument(contextPrefix != null, "null context not allowed");
    try {
      aggregatesTable.deleteAll(entityCodec.encodeWithoutPadding(MetricsEntityType.CONTEXT, contextPrefix));
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
      delete(contextPrefix, metricPrefix, "0", (String[]) null);
    }
  }

  /**
   * Deletes entries in the aggregate table that match the given context prefix, metric prefix, runId, and tag.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @param runId Runid to match.
   * @param tags Tags to match, null means any tag.
   * @throws OperationException if there is an error in deleting entries.
   */
  public void delete(String contextPrefix, String metricPrefix, String runId, String... tags)
    throws OperationException {
    byte[] startRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0);
    byte[] endRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0xff);
    try {
      aggregatesTable.deleteRange(startRow, endRow, tags == null ? null : Bytes.toByteArrays(tags),
                                  getFilter(contextPrefix, metricPrefix, runId));
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Scans the aggregate table for metrics without tag.
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scan(String contextPrefix, String metricPrefix) {
    return scan(contextPrefix, metricPrefix, null, MetricsConstants.EMPTY_TAG);
  }

  /**
   * Scans the aggregate table.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @param tagPrefix Prefix of tag to match. A null value will match untagged metrics, which is the same as passing in
   *                  MetricsConstants.EMPTY_TAG.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scan(String contextPrefix, String metricPrefix, String runId, String tagPrefix) {
    return scanFor(contextPrefix, metricPrefix, runId, tagPrefix == null ? MetricsConstants.EMPTY_TAG : tagPrefix);
  }

  /**
   * Scans the aggregate table for the given context and metric prefixes, and across all tags
   * including the empty tag.  Potentially expensive, use of this method should be avoided if the tag wanted is
   * known before hand.
   *
   * @param contextPrefix Prefix of context to match, a null value means any context.
   * @param metricPrefix Prefix of metric to match, a null value means any metric.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows.
   */
  public AggregatesScanner scanAllTags(String contextPrefix, String metricPrefix) {
    return scanFor(contextPrefix, metricPrefix, "0", null);
  }

  /**
   * Scans the aggregate table for its rows only.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scanRowsOnly(String contextPrefix, String metricPrefix) {
    return scanRowsOnly(contextPrefix, metricPrefix, null, MetricsConstants.EMPTY_TAG);
  }

  /**
   * Scans the aggregate table for its rows only.
   *
   * @param contextPrefix Prefix of context to match, null means any context.
   * @param metricPrefix Prefix of metric to match, null means any metric.
   * @param tagPrefix Prefix of tag to match.  A null value will match untagged metrics, which is the same as passing in
   *                  MetricsConstants.EMPTY_TAG.
   * @return A {@link AggregatesScanner} for iterating scan over matching rows
   */
  public AggregatesScanner scanRowsOnly(String contextPrefix, String metricPrefix, String runId, String tagPrefix) {
    byte[] startRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0);
    byte[] endRow = getRawPaddedKey(contextPrefix, metricPrefix, runId, 0xff);

    try {
      Scanner scanner = aggregatesTable.scan(startRow, endRow, null, getFilter(contextPrefix, metricPrefix, runId));
      return new AggregatesScanner(contextPrefix, metricPrefix, runId,
                                   tagPrefix == null ? MetricsConstants.EMPTY_TAG : tagPrefix, scanner, entityCodec);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Clears the storage table.
   * @throws OperationException If error in clearing data.
   */
  public void clear() throws OperationException {
    try {
      aggregatesTable.deleteAll(new byte[] { });
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  private AggregatesScanner scanFor(String contextPrefix, String metricPrefix, String runId, String tagPrefix) {
    byte[] startRow = getPaddedKey(contextPrefix, metricPrefix, runId, 0);
    byte[] endRow = getPaddedKey(contextPrefix, metricPrefix, runId, 0xff);
    try {
      // scan starting from start to end across all columns using a fuzzy filter for efficiency
      Scanner scanner = aggregatesTable.scan(startRow, endRow, null, getFilter(contextPrefix, metricPrefix, runId));
      return new AggregatesScanner(contextPrefix, metricPrefix, runId, tagPrefix, scanner, entityCodec);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private byte[] getKey(String context, String metric, String runId) {
    Preconditions.checkArgument(context != null, "Context cannot be null.");
    Preconditions.checkArgument(runId != null, "RunId cannot be null.");
    Preconditions.checkArgument(metric != null, "Metric cannot be null.");

    return Bytes.add(
      entityCodec.encode(MetricsEntityType.CONTEXT, context),
      entityCodec.encode(MetricsEntityType.METRIC, metric),
      entityCodec.encode(MetricsEntityType.RUN, runId)
    );
  }

  private byte[] getPaddedKey(String contextPrefix, String metricPrefix, String runId, int padding) {

    Preconditions.checkArgument(metricPrefix != null, "Metric cannot be null.");

    return getRawPaddedKey(contextPrefix, metricPrefix, runId, padding);
  }

  private byte[] getRawPaddedKey(String contextPrefix, String metricPrefix, String runId, int padding) {
    return Bytes.concat(
      entityCodec.paddedEncode(MetricsEntityType.CONTEXT, contextPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.METRIC, metricPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.RUN, runId, padding)
    );
  }

  private FuzzyRowFilter getFilter(String contextPrefix, String metricPrefix, String runId) {
    // Create fuzzy row filter
    ImmutablePair<byte[], byte[]> contextPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.CONTEXT,
                                                                              contextPrefix, 0);
    ImmutablePair<byte[], byte[]> metricPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.METRIC,
                                                                             metricPrefix, 0);
    ImmutablePair<byte[], byte[]> runIdPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.RUN, runId, 0);

    // Use a FuzzyRowFilter to select the row and the use ColumnPrefixFilter to select tag column.
    return new FuzzyRowFilter(ImmutableList.of(ImmutablePair.of(
      Bytes.concat(contextPair.getFirst(), metricPair.getFirst(), runIdPair.getFirst()),
      Bytes.concat(contextPair.getSecond(), metricPair.getSecond(), runIdPair.getSecond())
    )));
  }
}
