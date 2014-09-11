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
import co.cask.cdap.metrics.MetricsConstants;
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
    // The prefix is always have a "." suffix for unique matching for entity string, which uses "." as level separator
    // E.g. app.f.flowId would not match with app.f.flowId2, but match with app.f.flowId.flowletId
    this.contextPrefix = contextPrefix == null ? null : contextPrefix + ".";
    this.metricPrefix = metricPrefix == null ? null : metricPrefix + ".";
    this.runId = runId;
    this.tagPrefix = tagPrefix == null ? null : tagPrefix + ".";
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
        Row rowResult;
        while ((rowResult = scanner.next()) != null) {
          rowScanned++;
          byte[] rowKey = rowResult.getRow();

          // Decode context and metric from key
          int offset = 0;
          context = entityCodec.decode(MetricsEntityType.CONTEXT, rowKey, offset);
          // Always have a "." suffix for unique matching
          if (contextPrefix != null && !(context + ".").startsWith(contextPrefix)) {
            continue;
          }

          offset += entityCodec.getEncodedSize(MetricsEntityType.CONTEXT);
          metric = entityCodec.decode(MetricsEntityType.METRIC, rowKey, offset);
          // Always have a "." suffix for unique matching
          if (metricPrefix != null && !(metric + ".").startsWith(metricPrefix)) {
            continue;
          }

          offset += entityCodec.getEncodedSize(MetricsEntityType.METRIC);
          rid = entityCodec.decode(MetricsEntityType.RUN, rowKey, offset);
          if (runId != null && !runId.equals(rid)) {
            continue;
          }

          currentTag = rowResult.getColumns().entrySet().iterator();
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
          // Always have a "." suffix for unique matching
          if (tagPrefix != null && !(tag + ".").startsWith(tagPrefix)) {
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
