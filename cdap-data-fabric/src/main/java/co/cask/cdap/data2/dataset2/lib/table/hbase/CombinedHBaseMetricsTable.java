/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * Combined Metrics table to read from both v2 and v3 Tables and write to v3 table
 *
 * Writing:
 * When cluster is upgraded, the cluster will have both v2 and v3 tables. Now, all the new writes only goes to v3 table.
 * However, in cases where the row same column is already present in v2 table, for gauge metrics, we first delete that
 * column from v2 table and then write to v3 table. This approach is used to differentiate gauge and count metrics.
 *
 * Reading:
 * When we scan, we scan from both v2 and v3 tables using {@link CombinedMetricsScanner}. For higher resolution
 * tables, if the same row is divided between v2 and v3 tables, we merge the columns and add the values of
 * overlapping columns. This also works for gauges, because when writing to the v3 table, we delete the
 * corresponding entry in v2 table. Therefore we will never encounter a value in both tables for a gauge.
 *
 */
public class CombinedHBaseMetricsTable implements MetricsTable {
  private static final Logger LOG = LoggerFactory.getLogger(CombinedHBaseMetricsTable.class);
  private final MetricsTable v2HBaseTable;
  private final MetricsTable v3HBaseTable;
  private final int resolution;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseTableUtil hBaseTableUtil;
  private final Predicate tableExists;

  private boolean v2TableDeleted;

  public CombinedHBaseMetricsTable(final MetricsTable v2HBaseTable, MetricsTable v3HBaseTable,
                                   int tableResoluton, CConfiguration cConfiguration, Configuration hConf,
                                   HBaseTableUtil hBaseTableUtil) {
    this.v2HBaseTable = v2HBaseTable;
    this.v3HBaseTable = v3HBaseTable;
    this.resolution = tableResoluton;
    this.cConf = cConfiguration;
    this.hConf = hConf;
    this.hBaseTableUtil = hBaseTableUtil;
    this.v2TableDeleted = false;
    this.tableExists =  new Predicate<Integer>() {
      @Override
      public boolean apply(@Nullable Integer input) {
        if (!v2TableDeleted) {
          return v2MetricsTableExists();
        } else {
          return false;
        }
      }
    };
  }

   @VisibleForTesting
   CombinedHBaseMetricsTable(MetricsTable v2HBaseTable, MetricsTable v3HBaseTable,
                                   int tableResoluton, CConfiguration cConfiguration, Configuration hConf,
                                   HBaseTableUtil hBaseTableUtil, Predicate<Integer> tableExists) {
    this.v2HBaseTable = v2HBaseTable;
    this.v3HBaseTable = v3HBaseTable;
    this.resolution = tableResoluton;
    this.cConf = cConfiguration;
    this.hConf = hConf;
    this.hBaseTableUtil = hBaseTableUtil;
    this.v2TableDeleted = false;
    this.tableExists = tableExists;
  }

  @Nullable
  @Override
  public byte[] get(byte[] row, byte[] column) {
    return v3HBaseTable.get(row, column);
  }

  @Override
  public void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    if (tableExists.apply(resolution)) {
      deleteColumns(updates);
    }
    v3HBaseTable.put(updates);
  }

  private void deleteColumns(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    for (Map.Entry<byte[], ? extends SortedMap<byte[], Long>> entry : updates.entrySet()) {
      byte[] row = entry.getKey();
      Set<byte[]> columnSet = entry.getValue().keySet();
      byte[][] columns = columnSet.toArray(new byte[columnSet.size()][]);
      // Only delete from v2 table. This is because for gauge metrics, put operation gets translated into 2 operations
      // 1.) Delete column from v2 table if it exists
      // 2.) Put column in v3 table.
      // This is the way to differentiate gauge and count metrics. So that when we scan both tables we know all the
      // remaining columns in v2 table are increments so just sum them up
      v2HBaseTable.delete(row, columns);
    }
  }

  @Override
  public void putBytes(SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> updates) {
    v3HBaseTable.putBytes(updates);
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    return v3HBaseTable.swap(row, column, oldValue, newValue);
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) {
    v3HBaseTable.increment(row, increments);
  }

  @Override
  public void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) {
    v3HBaseTable.increment(updates);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) {
    // This method will not be called from FactTable
    return v3HBaseTable.incrementAndGet(row, column, delta);
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    v3HBaseTable.delete(row, columns);
    if (tableExists.apply(resolution)) {
      v2HBaseTable.delete(row, columns);
    }
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue) {
    return v3HBaseTable.checkAndPut(row, column, expectedValue, newValue);
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable FuzzyRowFilter filter) {
    Scanner v2Scan = null;
    if (tableExists.apply(resolution)) {
      v2Scan = v2HBaseTable.scan(start, stop, filter);
    }
    Scanner v3Scan = v3HBaseTable.scan(start, stop, filter);
    return new CombinedMetricsScanner(v2Scan, v3Scan);
  }

  @Override
  public void close() throws IOException {
    v3HBaseTable.close();
    if (tableExists.apply(resolution)) {
      v2HBaseTable.close();
    }
  }

  private TableId getV2MetricsTableName(int resolution) {
    String v2TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                   Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
    return TableId.from(NamespaceId.SYSTEM.getNamespace(), v2TableName);
  }

  /**
   * check if v2 metrics table exists for the resolution
   * @return true if table exists; false otherwise
   */
  public boolean v2MetricsTableExists() {
    TableId tableId = getV2MetricsTableName(resolution);
    try {
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        TableId hBaseTableId =
          hBaseTableUtil.createHTableId(new NamespaceId(tableId.getNamespace()), tableId.getTableName());
        boolean doesExist  = hBaseTableUtil.tableExists(admin, hBaseTableId);
        if (!doesExist) {
          v2TableDeleted = true;
        }
        return doesExist;
      }
    } catch (IOException e) {
      LOG.warn("Exception while checking table exists", e);
    }
    return false;
  }
}
