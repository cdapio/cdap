package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.continuuity.metrics.MetricsConstants;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.Map;

/**
 * A scanner for iterating over scanning result of {@link AggregatesTable}.
 */
public class AggregatesScanner implements Iterator<AggregatesScanResult> {

  private final String contextPrefix;
  private final String metricPrefix;
  private final String runId;
  private final String tagPrefix;
  private final Scanner scanner;
  private final MetricsEntityCodec entityCodec;
  private final Iterator<AggregatesScanResult> internalIterator;

  private int rowScanned;

  AggregatesScanner(String contextPrefix, String metricPrefix, String runId, String tagPrefix,
                    Scanner scanner, MetricsEntityCodec entityCodec) {

    this.contextPrefix = contextPrefix;
    this.metricPrefix = metricPrefix;
    this.runId = runId;
    this.tagPrefix = tagPrefix;
    this.scanner = scanner;
    this.entityCodec = entityCodec;
    this.internalIterator = createIterator();
  }

  @Override
  public boolean hasNext() {
    return internalIterator.hasNext();
  }

  @Override
  public AggregatesScanResult next() {
    return internalIterator.next();
  }

  @Override
  public void remove() {
    internalIterator.remove();
  }

  public void close() {
    scanner.close();
  }

  public int getRowScanned() {
    return rowScanned;
  }

  private Iterator<AggregatesScanResult> createIterator() {
    return new AbstractIterator<AggregatesScanResult>() {

      private String context;
      private String metric;
      private String rid;
      private Iterator<Map.Entry<byte[], byte[]>> currentTag = null;

      @Override
      protected AggregatesScanResult computeNext() {
        // If a row already scanned, find from the scanned value the next tag.
        AggregatesScanResult result = findNextResult();
        if (result != null) {
          return result;
        }

        // If either no row has been scanned or already exhausted all columns from previous scan, find the next row.
        ImmutablePair<byte[], Map<byte[], byte[]>> rowResult;
        while ((rowResult = scanner.next()) != null) {
          rowScanned++;
          byte[] rowKey = rowResult.getFirst();

          // Decode context and metric from key
          int offset = 0;
          context = entityCodec.decode(MetricsEntityType.CONTEXT, rowKey, offset);
          if (contextPrefix != null && !context.startsWith(contextPrefix)) {
            continue;
          }

          offset += entityCodec.getEncodedSize(MetricsEntityType.CONTEXT);
          metric = entityCodec.decode(MetricsEntityType.METRIC, rowKey, offset);
          if (metricPrefix != null && !metric.startsWith(metricPrefix)) {
            continue;
          }

          offset += entityCodec.getEncodedSize(MetricsEntityType.METRIC);
          rid = entityCodec.decode(MetricsEntityType.RUN, rowKey, offset);
          if (runId != null && !runId.equals(rid)) {
            continue;
          }

          currentTag = rowResult.getSecond().entrySet().iterator();
          result = findNextResult();
          if (result != null) {
            return result;
          }
        }

        // No more data
        scanner.close();
        return endOfData();
      }

      private AggregatesScanResult findNextResult() {
        while (currentTag != null && currentTag.hasNext()) {
          Map.Entry<byte[], byte[]> tagValue = currentTag.next();
          String tag = Bytes.toString(tagValue.getKey());
          if (tagPrefix != null && !tag.startsWith(tagPrefix)) {
            continue;
          }
          if (MetricsConstants.EMPTY_TAG.equals(tag)) {
            tag = null;
          }
          return new AggregatesScanResult(context, metric, rid, tag, Bytes.toLong(tagValue.getValue()));
        }
        return null;
      }
    };
  }
}
