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
  public Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    try {
      Optional<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = structuredTable.read(keys);
      } else {
        long curTime = System.nanoTime();
        result = structuredTable.read(keys);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "read.time", duration);
      }
      metricsCollector.increment(metricPrefix + "read.count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "read.error", 1L);
      throw e;
    }
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
                                      Collection<String> columns) throws InvalidFieldException, IOException {
    try {
      Optional<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = structuredTable.read(keys, columns);
      } else {
        long curTime = System.nanoTime();
        result = structuredTable.read(keys, columns);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "read.time", duration);
      }
      metricsCollector.increment(metricPrefix + "read.count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "read.error", 1L);
      throw e;
    }
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

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException, IOException {
    try {
      CloseableIterator<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = structuredTable.scan(keyRange, limit);
      } else {
        long curTime = System.nanoTime();
        result = structuredTable.scan(keyRange, limit);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "scan.time", duration);
      }
      metricsCollector.increment(metricPrefix + "scan.count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "scan.error", 1L);
      throw e;
    }
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Field<?> index) throws InvalidFieldException, IOException {
    try {
      CloseableIterator<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = structuredTable.scan(index);
      } else {
        long curTime = System.nanoTime();
        result = structuredTable.scan(index);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "index.scan.time", duration);
      }
      metricsCollector.increment(metricPrefix + "index.scan.count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "index.scan.error", 1L);
      throw e;
    }
  }

  @Override
  public CloseableIterator<StructuredRow> multiScan(Collection<Range> keyRanges,
                                                    int limit) throws InvalidFieldException, IOException {
    try {
      CloseableIterator<StructuredRow> result;
      if (!emitTimeMetrics) {
        result = structuredTable.multiScan(keyRanges, limit);
      } else {
        long curTime = System.nanoTime();
        result = structuredTable.multiScan(keyRanges, limit);
        long duration = System.nanoTime() - curTime;
        metricsCollector.increment(metricPrefix + "multi.scan.time", duration);
      }
      metricsCollector.increment(metricPrefix + "multi.scan.count", 1L);
      return result;
    } catch (Exception e) {
      metricsCollector.increment(metricPrefix + "multi.scan.error", 1L);
      throw e;
    }
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
  public void close() throws IOException {
    structuredTable.close();
  }
}
