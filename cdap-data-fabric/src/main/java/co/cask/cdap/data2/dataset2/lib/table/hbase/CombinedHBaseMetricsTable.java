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

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
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

  private final MetricsTable v3HBaseTable;
  private final int resolution;
  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;

  private MetricsTable v2HBaseTable;

  public CombinedHBaseMetricsTable(MetricsTable v2HBaseTable, MetricsTable v3HBaseTable,
                                   int tableResoluton, CConfiguration cConfiguration,
                                   DatasetFramework datasetFramework) {
    this.v2HBaseTable = v2HBaseTable;
    this.v3HBaseTable = v3HBaseTable;
    this.resolution = tableResoluton;
    this.cConf = cConfiguration;
    this.datasetFramework = datasetFramework;
  }


  @Nullable
  @Override
  public byte[] get(byte[] row, byte[] column) {
    return v3HBaseTable.get(row, column);
  }

  @Override
  public void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    deleteColumns(updates);
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
      if (v2HBaseTable != null) {
        try {
          v2HBaseTable.delete(row, columns);
        } catch (Exception e) {
          handleV2TableException(e, "Delete", getV2MetricsTableDatasetId().getDataset());
        }
      }
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
    if (v2HBaseTable != null) {
      try {
        v2HBaseTable.delete(row, columns);
      } catch (Exception e) {
        handleV2TableException(e, "Delete", getV2MetricsTableDatasetId().getDataset());
      }
    }
  }

  /**
   * If the exception is of type IOException,
   * so we check if the table exists, if it doesn't exist (deleted in-between) we close the table reference and set
   * it to null, else we propagate the exception
   *
   * @param operation operation that failed
   */
  private void handleV2TableException(Exception e, String operation, String tableName) {
    if (e instanceof DataSetException || e instanceof RuntimeException) {
      if (e.getCause() instanceof IOException) {
        try {
          if (!v2MetricsTableExists()) {
            Closeables.closeQuietly(v2HBaseTable);
            v2HBaseTable = null;
            return;
          }
        } catch (DatasetManagementException dme) {
          LOG.error("{} operation failed on table {}", operation, tableName, dme);
          throw Throwables.propagate(e);
        }
      }
    }
    // throw if any other exception or if the table exists
    throw Throwables.propagate(e);
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable FuzzyRowFilter filter) {
    Scanner v2Scan = null;
    if (v2HBaseTable != null) {
      try {
        v2Scan = v2HBaseTable.scan(start, stop, filter);
      } catch (Exception e) {
        handleV2TableException(e, "Scan", getV2MetricsTableDatasetId().getDataset());
      }
    }

    Scanner v3Scan = v3HBaseTable.scan(start, stop, filter);
    return new CombinedMetricsScanner(v2Scan, v3Scan, getV2MetricsTableDatasetId(), datasetFramework);
  }

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(v3HBaseTable);
    Closeables.closeQuietly(v2HBaseTable);
  }

  private DatasetId getV2MetricsTableDatasetId() {
    String v2TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                   Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
    return NamespaceId.SYSTEM.dataset(v2TableName);
  }

  /**
   * check if v2 metrics table exists for the resolution
   * @return true if table exists; false otherwise
   */
  public boolean v2MetricsTableExists() throws DatasetManagementException {
    return datasetFramework.hasInstance(getV2MetricsTableDatasetId());
  }
}
