package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public final class MetricScanner implements Iterator<MetricScanResult> {

  private static final Logger LOG = LoggerFactory.getLogger(MetricScanner.class);

  private static final Predicate<Map.Entry<byte[], byte[]>> COLUMN_FILTER = new Predicate<Map.Entry<byte[], byte[]>>() {
    @Override
    public boolean apply(Map.Entry<byte[], byte[]> input) {
      byte[] column = input.getKey();
      return !Arrays.equals(MetricConstants.CONTEXT, column) && !Arrays.equals(MetricConstants.METRIC, column);
    }
  };

  private final MetricScanQuery query;
  private final Scanner scanner;
  private final MetricEntityCodec entityCodec;
  private final int resolution;

  // Track the number of row scanned through the iterator. It's for reporting and debugging purpose.
  private int rowScanned;

  // Use an internal iterator to avoid leaking AbstractIterator methods to outside.
  private final Iterator<MetricScanResult> internalIterator;

  /**
   * Construct a MetricScanner. Should only be called by MetricTable.
   * @param query The query used to create this scanner.
   * @param scanner The table scanner of a query.
   */
  MetricScanner(MetricScanQuery query, Scanner scanner, MetricEntityCodec entityCodec, int resolution) {
    this.query = query;
    this.scanner = scanner;
    this.entityCodec = entityCodec;
    this.resolution = resolution;
    this.internalIterator = createIterator();
  }

  public void close() {
    scanner.close();
  }

  public int getRowScanned() {
    return rowScanned;
  }

  @Override
  public boolean hasNext() {
    return internalIterator.hasNext();
  }

  @Override
  public MetricScanResult next() {
    return internalIterator.next();
  }

  @Override
  public void remove() {
    internalIterator.remove();
  }

  private Iterator<MetricScanResult> createIterator() {
    return new AbstractIterator<MetricScanResult>() {
      @Override
      protected MetricScanResult computeNext() {
        ImmutablePair<byte[], Map<byte[], byte[]>> rowResult;
        while ((rowResult = scanner.next()) != null) {
          rowScanned++;
          byte[] rowKey = rowResult.getFirst();
          Map<byte[], byte[]> columnValue = rowResult.getSecond();

          // Decode context and metric from columns
          byte[] encodedContext = columnValue.get(MetricConstants.CONTEXT);
          if (encodedContext == null) {
            LOG.warn("Missing context column. Metric row ignored.");
            continue;
          }
          byte[] encodedMetric = columnValue.get(MetricConstants.METRIC);
          if (encodedMetric == null) {
            LOG.warn("Missing metric column. Metric row ignored.");
            continue;
          }

          String context = entityCodec.decode(MetricEntityType.CONTEXT, encodedContext);
          if (!context.startsWith(query.getContextPrefix())) {
            continue;
          }

          String metric = entityCodec.decode(MetricEntityType.METRIC, encodedMetric);
          if (!metric.startsWith(query.getMetricPrefix())) {
            continue;
          }

          int offset = encodedContext.length + encodedMetric.length;
          String tag = entityCodec.decode(MetricEntityType.TAG, rowKey, offset, rowKey.length - offset - 4);
          // If there is no tag in the key, query shouldn't have tag
          if (tag.equals(MetricConstants.EMPTY_TAG) && query.getTagPrefix() != null) {
            continue;
          }
          // If there is tag in the query, it must match with the row key.
          if (query.getTagPrefix() != null && !tag.startsWith(query.getTagPrefix())) {
            continue;
          }

          // Last 4 bytes is the timebase.
          int timeBase = Bytes.toInt(rowKey, rowKey.length - 4, 4);

          return new MetricScanResult(context, metric, tag.equals(MetricConstants.EMPTY_TAG) ? null : tag,
                                      new TimeValueIterable(timeBase, resolution,
                                                            query.getStartTime(), query.getEndTime(),
                                                            Iterables.filter(columnValue.entrySet(), COLUMN_FILTER)));
        }

        scanner.close();
        return endOfData();
      }
    };
  }
}
