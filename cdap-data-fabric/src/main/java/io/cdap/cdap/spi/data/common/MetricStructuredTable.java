/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.common;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Range;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * Structured table that takes a delegation and emit metrics on each operation.
 */
public class MetricStructuredTable implements StructuredTable {

  private final StructuredTable structuredTable;
  private final MetricsCollector metricsCollector;
  private final String metricPrefix;
  private final boolean emitTimeMetrics;

  public MetricStructuredTable(StructuredTableId tableId,
      StructuredTable structuredTable, MetricsCollector metricsCollector,
      boolean emitTimeMetrics) {
    this.structuredTable = structuredTable;
    this.metricsCollector = metricsCollector;
    this.metricPrefix = Constants.Metrics.StructuredTable.METRICS_PREFIX + tableId.getName() + ".";
    this.emitTimeMetrics = emitTimeMetrics;
  }

  @Override
  public void upsert(Collection<Field<?>> fields) throws InvalidFieldException, IOException {
    try {
      if (!emitTimeMetrics) {
        structuredTable.upsert(fields);
      } else {
        long curTime = System.nanoTime();
        structuredTable.upsert(fields);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "upsert.time", duration);
      }
      metricsCollector.increment(metricPrefix + "upsert.count", 1L);
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "upsert.error", 1L);
      throw e;
    }
  }

  @Override
  public void update(Collection<Field<?>> fields) throws InvalidFieldException, IOException {
    try {
      if (!emitTimeMetrics) {
        structuredTable.update(fields);
      } else {
        long curTime = System.nanoTime();
        structuredTable.update(fields);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "update.time", duration);
      }
      metricsCollector.increment(metricPrefix + "update.count", 1L);
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "update.error", 1L);
      throw e;
    }
  }

  private interface ReadFunction {

    Optional<StructuredRow> read() throws InvalidFieldException, IOException;
  }

  private Optional<StructuredRow> read(ReadFunction readFunc, String metricNamePrefix)
      throws IOException {
    try {
      Optional<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = readFunc.read();
      } else {
        long curTime = System.nanoTime();
        result = readFunc.read();
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + metricNamePrefix + "time", duration);
      }
      metricsCollector.increment(metricPrefix + metricNamePrefix + "count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + metricNamePrefix + "error", 1L);
      throw e;
    }
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys)
      throws InvalidFieldException, IOException {
    return read(() -> structuredTable.read(keys), "read.");
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
      Collection<String> columns) throws InvalidFieldException, IOException {
    return read(() -> structuredTable.read(keys, columns), "read.");
  }

  @Override
  public Collection<StructuredRow> multiRead(Collection<? extends Collection<Field<?>>> multiKeys)
      throws InvalidFieldException, IOException {
    try {
      if (!emitTimeMetrics) {
        return structuredTable.multiRead(multiKeys);
      }
      long curTime = System.nanoTime();
      Collection<StructuredRow> result = structuredTable.multiRead(multiKeys);
      long duration = System.nanoTime() - curTime;
      metricsCollector.increment(metricPrefix + "multi.read.time", duration);
      metricsCollector.increment(metricPrefix + "multi.read.count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "multi.read.error", 1L);
      throw e;
    }
  }

  private interface ScanFunction {

    CloseableIterator<StructuredRow> scan() throws InvalidFieldException, IOException;
  }

  private CloseableIterator<StructuredRow> scan(ScanFunction scanFunc, String metricNamePrefix)
      throws IOException {
    try {
      CloseableIterator<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = scanFunc.scan();
      } else {
        long curTime = System.nanoTime();
        result = scanFunc.scan();
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + metricNamePrefix + "time", duration);
      }
      metricsCollector.increment(metricPrefix + metricNamePrefix + "count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + metricNamePrefix + "error", 1L);
      throw e;
    }
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit, SortOrder sortOrder)
      throws InvalidFieldException, IOException {
    return scan(() -> structuredTable.scan(keyRange, limit, sortOrder), "scan.");
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit)
      throws InvalidFieldException, IOException {
    return scan(keyRange, limit, SortOrder.ASC);
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Field<?> index)
      throws InvalidFieldException, IOException {
    return scan(() -> structuredTable.scan(index), "index.scan.");
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit,
      Collection<Field<?>> filterIndexes)
      throws InvalidFieldException, IOException {
    return scan(() -> structuredTable.scan(keyRange, limit, filterIndexes), "index.range.scan.");
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit, String orderByField,
      SortOrder sortOrder)
      throws InvalidFieldException, IOException {
    return scan(() -> structuredTable.scan(keyRange, limit, orderByField, sortOrder),
        "sort.range.scan.");
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit,
      Collection<Field<?>> filterIndexes, SortOrder sortOrder)
      throws InvalidFieldException, IOException {
    return scan(() -> structuredTable.scan(keyRange, limit, filterIndexes, sortOrder),
        "sort.index.range.scan.");
  }

  private interface MultiScanFunction {

    CloseableIterator<StructuredRow> multiScan() throws InvalidFieldException, IOException;
  }

  private CloseableIterator<StructuredRow> multiScan(MultiScanFunction multiScanFunction,
      String metricNamePrefix)
      throws IOException {
    try {
      CloseableIterator<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = multiScanFunction.multiScan();
      } else {
        long curTime = System.nanoTime();
        result = multiScanFunction.multiScan();
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + metricNamePrefix + "time", duration);
      }
      metricsCollector.increment(metricPrefix + metricNamePrefix + "count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + metricNamePrefix + "error", 1L);
      throw e;
    }
  }

  @Override
  public CloseableIterator<StructuredRow> multiScan(Collection<Range> keyRanges,
      int limit) throws InvalidFieldException, IOException {
    return multiScan(() -> structuredTable.multiScan(keyRanges, limit), "multi.scan.");
  }

  @Override
  public boolean compareAndSwap(Collection<Field<?>> keys, Field<?> oldValue, Field<?> newValue)
      throws InvalidFieldException, IOException, IllegalArgumentException {
    try {
      boolean result;
      if (!emitTimeMetrics) {
        result = structuredTable.compareAndSwap(keys, oldValue, newValue);
      } else {
        long curTime = System.nanoTime();
        result = structuredTable.compareAndSwap(keys, oldValue, newValue);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "compareAndSwap.time", duration);
      }
      metricsCollector.increment(metricPrefix + "compareAndSwap.count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "compareAndSwap.error", 1L);
      throw e;
    }
  }

  @Override
  public void increment(Collection<Field<?>> keys, String column,
      long amount) throws InvalidFieldException, IOException, IllegalArgumentException {
    try {
      if (!emitTimeMetrics) {
        structuredTable.increment(keys, column, amount);
      } else {
        long curTime = System.nanoTime();
        structuredTable.increment(keys, column, amount);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "increment.time", duration);
      }
      metricsCollector.increment(metricPrefix + "increment.count", 1L);
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "increment.error", 1L);
      throw e;
    }
  }

  @Override
  public void delete(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    try {
      if (!emitTimeMetrics) {
        structuredTable.delete(keys);
      } else {
        long curTime = System.nanoTime();
        structuredTable.delete(keys);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "delete.time", duration);
      }
      metricsCollector.increment(metricPrefix + "delete.count", 1L);
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "delete.error", 1L);
      throw e;
    }
  }

  @Override
  public void deleteAll(Range keyRange) throws InvalidFieldException, IOException {
    try {
      if (!emitTimeMetrics) {
        structuredTable.deleteAll(keyRange);
      } else {
        long curTime = System.nanoTime();
        structuredTable.deleteAll(keyRange);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "deleteAll.time", duration);
      }
      metricsCollector.increment(metricPrefix + "deleteAll.count", 1L);
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "deleteAll.error", 1L);
      throw e;
    }
  }

  @Override
  public void scanDeleteAll(Range keyRange)
      throws InvalidFieldException, UnsupportedOperationException, IOException {
    try {
      if (!emitTimeMetrics) {
        structuredTable.scanDeleteAll(keyRange);
      } else {
        long curTime = System.nanoTime();
        structuredTable.scanDeleteAll(keyRange);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "scanDeleteAll.time", duration);
      }
      metricsCollector.increment(metricPrefix + "scanDeleteAll.count", 1L);
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "scanDeleteAll.error", 1L);
      throw e;
    }
  }

  @Override
  public void updateAll(Range keyRange, Collection<Field<?>> fields)
      throws InvalidFieldException, IOException {
    try {
      if (!emitTimeMetrics) {
        structuredTable.updateAll(keyRange, fields);
      } else {
        long curTime = System.nanoTime();
        structuredTable.updateAll(keyRange, fields);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "updateAll.time", duration);
      }
      metricsCollector.increment(metricPrefix + "updateAll.count", 1L);
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "updateAll.error", 1L);
      throw e;
    }
  }

  @Override
  public long count(Collection<Range> keyRanges) throws IOException {
    try {
      long count = 0;
      if (!emitTimeMetrics) {
        count = structuredTable.count(keyRanges);
      } else {
        long curTime = System.nanoTime();
        count = structuredTable.count(keyRanges);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "count.time", duration);
      }
      metricsCollector.increment(metricPrefix + "count.count", 1L);
      return count;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "count.error", 1L);
      throw e;
    }
  }

  @Override
  public long count(Collection<Range> keyRanges,
      Collection<Field<?>> filterIndexes) throws IOException {
    try {
      long count = 0;
      if (!emitTimeMetrics) {
        count = structuredTable.count(keyRanges, filterIndexes);
      } else {
        long curTime = System.nanoTime();
        count = structuredTable.count(keyRanges, filterIndexes);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "count.time", duration);
      }
      metricsCollector.increment(metricPrefix + "count.count", 1L);
      return count;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "count.error", 1L);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    structuredTable.close();
  }
}
