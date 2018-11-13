/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.PrefixedNamespaces;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * A metrics table client based on leveldb.
 */
public class LevelDBMetricsTable implements MetricsTable {
  private static final Logger LOG = LoggerFactory.getLogger(LevelDBMetricsTable.class);

  private static final Function<Long, byte[]> LONG_TO_BYTES = new Function<Long, byte[]>() {
    @Override
    public byte[] apply(Long input) {
      return Bytes.toBytes(input);
    }
  };
  private static final Function<SortedMap<byte[], Long>, SortedMap<byte[], byte[]>>
    TRANSFORM_MAP_LONG_TO_BYTE_ARRAY = new Function<SortedMap<byte[], Long>, SortedMap<byte[], byte[]>>() {
    @Override
    public SortedMap<byte[], byte[]> apply(SortedMap<byte[], Long> input) {
      return Maps.transformValues(input, LONG_TO_BYTES);
    }
  };

  private final String tableName;
  private final LevelDBTableCore core;
  private long count;
  private long timeConsumed;

  public LevelDBMetricsTable(DatasetContext datasetContext, String tableName,
                             LevelDBTableService service, CConfiguration cConf) throws IOException {
    this.core = new LevelDBTableCore(PrefixedNamespaces.namespace(cConf, datasetContext.getNamespaceId(), tableName),
                                     service);
    this.tableName = tableName;
  }

  @Override
  public long getCount() {
    return count;
  }

  @Override
  public long getWriteTime() {
    return timeConsumed;
  }

  @Override
  public byte[] get(byte[] row, byte[] column) {
    try {
      NavigableMap<byte[], byte[]> result = core.getRow(row, new byte[][]{column}, null, null, -1, null);
      if (!result.isEmpty()) {
        return result.get(column);
      }
      return null;
    } catch (IOException e) {
      throw new DataSetException("Get failed on table " + tableName, e);
    }
  }

  // yaojie gauge
  @Override
  public void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    long startTime = System.currentTimeMillis();
    SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> convertedUpdates =
      Maps.transformValues(updates, TRANSFORM_MAP_LONG_TO_BYTE_ARRAY);
    try {
      core.persist(convertedUpdates, 0L);
      count++;
    } catch (IOException e) {
      throw new DataSetException("Put failed on table " + tableName, e);
    }
    timeConsumed += System.currentTimeMillis() - startTime;
  }

  @Override
  public void putBytes(SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> updates) {
    try {
      core.persist(updates, System.currentTimeMillis());
    } catch (IOException e) {
      throw new DataSetException("Put failed on table " + tableName, e);
    }
  }

  public long getWriteTimeDB() {
    return core.getWriteTime();
  }

  public long getReadTimeDB() {
    return core.getReadTime();
  }

  public long getMapSizeDB() {
    return core.getMapSize();
  }

  @Override
  public synchronized boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    try {
      return core.swap(row, column, oldValue, newValue);
    } catch (IOException e) {
      throw new DataSetException("Swap failed on table " + tableName, e);
    }
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) {
    long startTime = System.currentTimeMillis();
    try {
      count++;
      core.increment(row, increments);
    } catch (IOException e) {
      throw new DataSetException("Increment failed on table " + tableName, e);
    }
    timeConsumed += System.currentTimeMillis() - startTime;
  }

  // yaojie counter
  @Override
  public void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) {
    long startTime = System.currentTimeMillis();
    try {
      count++;
      core.increment(updates);
    } catch (IOException e) {
      throw new DataSetException("Increment failed on table " + tableName, e);
    }
    timeConsumed += System.currentTimeMillis() - startTime;
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) {
    long startTime = System.currentTimeMillis();
    try {
      count++;
      long result = core.increment(row, ImmutableMap.of(column, delta)).get(column);
      timeConsumed += System.currentTimeMillis() - startTime;
      return result;
    } catch (IOException e) {
      throw new DataSetException("IncrementAndGet failed on table " + tableName, e);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    try {
      for (byte[] column : columns) {
        core.deleteColumn(row, column);
      }
    } catch (IOException e) {
      throw new DataSetException("Delete failed on table " + tableName, e);
    }
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop,
                      @Nullable FuzzyRowFilter filter) {
    try {
      return core.scan(start, stop, filter, null, null);
    } catch (IOException e) {
      throw new DataSetException("Scan failed on table " + tableName, e);
    }
  }

  @Override
  public void close() throws IOException {
    // Do nothing
  }

}
