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
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * perform table data migration from v2 metrics table to v3.
 */
public class  MetricsTableMigration {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableMigration.class);

  private final String v2MetricsTableName;
  private final String v3MetricsTableName;
  private final MetricsTable v2MetricsTable;
  private final MetricsTable v3MetricsTable;

  private volatile boolean stopping;

  public MetricsTableMigration(String v2MetricsTableName,
                               MetricsTable v2MetricsTable, String v3MetricsTableName, MetricsTable v3MetricsTable) {
    this.v2MetricsTableName = v2MetricsTableName;
    this.v2MetricsTable = v2MetricsTable;
    this.v3MetricsTableName = v3MetricsTableName;
    this.v3MetricsTable = v3MetricsTable;
    this.stopping = false;
  }

  /**
   * set this to stop transferring data and exit after completing the current processing
   */
  public void requestStop() {
    this.stopping = true;
  }

  /**
   * transfers data from v2 metrics table to v3 metrics table, limit on number of records is specified by
   * maxRecordsToScan
   * @param sleepMillisBetweenRecords time in milliseconds to sleep between each record transfer
   * @throws InterruptedException if it was interrupted when sleeping between record transfer
   */
  public void transferData(int sleepMillisBetweenRecords) throws InterruptedException {
    try (Scanner scanner = v2MetricsTable.scan(null, null, null)) {
      LOG.info("Starting Metrics Data Migration from {} to {} with {}ms sleep between records",
               v2MetricsTableName, v3MetricsTableName, sleepMillisBetweenRecords);
      Row row;
      int recordsScanned = 0;
      Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();
      while (!stopping && ((row = scanner.next()) != null)) {
        if (recordsScanned % 1000 == 0) {
          LOG.debug("Took {}s for transferring {} records in Metrics Data Migration",
                    stopwatch.elapsedTime(TimeUnit.SECONDS), recordsScanned);
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
          // if we are stopping; skip the column iteration and proceed to next step
          if (stopping) {
            break;
          }
          // column is timestamp, do a get on the new table
          byte[] value = v3MetricsTable.get(rowKey, entry.getKey());
          if (value == null) {
            // we perform checkAndPut for the new value, if the value was updated by the new upcoming metrics,
            // if it was a gauge, the processing would have deleted the entry from the old table
            // if it was a increment, we would perform increment in the next iteration of the migration run
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
        if (!stopping) {
          TimeUnit.MILLISECONDS.sleep(sleepMillisBetweenRecords);
        }
      }
      stopwatch.stop();
      LOG.info("Finished migrating {} records from the metrics table {}. Took {}s",
               recordsScanned, v2MetricsTableName, stopwatch.elapsedTime(TimeUnit.SECONDS));
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
