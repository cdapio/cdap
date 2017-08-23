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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * perform table data migration from v2 metrics table to v3.
 */
public class  MetricsTableMigration {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableMigration.class);

  private final MetricsTable v2MetricsTable;
  private final MetricsTable v3MetricsTable;

  public MetricsTableMigration(MetricsTable v2MetricsTable, MetricsTable v3MetricsTable) {
    this.v2MetricsTable = v2MetricsTable;
    this.v3MetricsTable = v3MetricsTable;
  }

  /**
   * returns true if old metrics data is still available for transfer
   * @return
   */
  public boolean isOldMetricsDataAvailable() {
    try (Scanner scanner = v2MetricsTable.scan(null, null, null)) {
      Row row = scanner.next();
      return (row != null);
    }
  }

  /**
   * transfers data from v2 metrics table to v3 metrics table, limit on number of records is specified by
   * maxRecordsToScan
   * @param sleepMillisBetweenRecords time in milliseconds to sleep between each record transfer
   * @throws InterruptedException if it was interrupted when sleeping between record transfer
   */
  public void transferData(int sleepMillisBetweenRecords) throws InterruptedException {
    try (Scanner scanner = v2MetricsTable.scan(null, null, null)) {
      LOG.trace("Starting Metrics Data Migration with {} ms sleep between records",
                sleepMillisBetweenRecords);
      Row row;
      int recordsScanned = 0;
      while (((row = scanner.next()) != null)) {
        if (recordsScanned % 10 == 0) {
          LOG.trace("Scanned {} records in Metrics Data Migration", recordsScanned);
        }

        byte[] rowKey = row.getRow();
        Map<byte[], byte[]> columns = row.getColumns();
        //row-map
        NavigableMap<byte[], NavigableMap<byte[], Long>> rowIncrements = new TreeMap<>(Bytes.BYTES_COMPARATOR);

        // column-map gauges
        List<byte[]> gaugeDeletes = new ArrayList<>();
        // column-map increments
        NavigableMap<byte[], Long> increments = new TreeMap<>(Bytes.BYTES_COMPARATOR);

        for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
          // column is timestamp, do a get on the new table
          byte[] value = v3MetricsTable.get(rowKey, entry.getKey());
          if (value == null) {
            // we perform checkAndPut for the new value, if the value was updated by the new upcoming metrics,
            // if it was a gauge, the processing would have deleted the entry from the old table
            // if it was a incrment, we would perform increment in the next iteration of the migration run
            // so its safe to skip delete on checkAndPut failure
            if (v3MetricsTable.swap(rowKey, entry.getKey(), null, entry.getValue())) {
              gaugeDeletes.add(entry.getKey());
            } else {
              LOG.trace("Swap operation failed for rowkey {} column {} with new value {}",
                        rowKey, entry.getKey(), entry.getValue());
            }
          } else {
            increments.put(entry.getKey(), Bytes.toLong(entry.getValue()));
          }
        }

        byte[][] deletes = getByteArrayFromSets(increments.keySet(), gaugeDeletes);

        // delete entries from old table
        if (deletes.length > 0) {
          v2MetricsTable.delete(rowKey, deletes);
        }

        // increments
        if (!increments.isEmpty()) {
          rowIncrements.put(rowKey, increments);
          v3MetricsTable.increment(rowIncrements);
        }
        recordsScanned++;
        TimeUnit.MILLISECONDS.sleep(sleepMillisBetweenRecords);
      }
      LOG.debug("Migrated {} records from the metrics table {}", recordsScanned, v2MetricsTable);
    }
  }

  private byte[][] getByteArrayFromSets(Set<byte[]> incrementSet, List<byte[]> gaugeDelets) {
    byte [][] deletes = new byte[incrementSet.size() + gaugeDelets.size()][];
    int index = 0;
    for (byte[] column : incrementSet) {
      deletes[index++] = column;
    }
    for (byte[] column : gaugeDelets) {
      deletes[index++] = column;
    }
    return deletes;
  }
}
