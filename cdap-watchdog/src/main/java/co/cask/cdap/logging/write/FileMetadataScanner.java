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

package co.cask.cdap.logging.write;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.logging.meta.LoggingStoreTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileMetadataScanner {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetadataScanner.class);
  private static final byte VERSION = 1;
  private static final byte[] ROW_KEY_PREFIX = LoggingStoreTableUtil.OLD_FILE_META_ROW_KEY_PREFIX;
  private static final byte[] ROW_KEY_PREFIX_END = Bytes.stopKeyForPrefix(ROW_KEY_PREFIX);
  private static final int MAX_METADATA_ENTRIES = 100;
  private static final int MAX_METADATA_COLUMNS_SCANNED = 1000;

  private final Transactional transactional;
  private final DatasetManager datasetManager;

  public FileMetadataScanner(DatasetManager datasetManager, Transactional transactional) {
    this.transactional = transactional;
    this.datasetManager = datasetManager;
  }

  /**
   * scans for meta data in old format which has expired the log retention.
   */
  public int scanAndDeleteOldMetaData(final long tillTime) throws Exception {
    return Transactions.execute(transactional, new TxCallable<Integer>() {
      @Override
      public Integer call(DatasetContext context) throws Exception {
        Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
        // create range with tillTime as endColumn
        Scan scan = new Scan(ROW_KEY_PREFIX, ROW_KEY_PREFIX_END, null);
        int entriesDeleted = 0;
        try (Scanner scanner = table.scan(scan)) {
          Row row;
          while ((row = scanner.next()) != null) {
            byte[] rowKey = row.getRow();
            for (Map.Entry<byte[], byte[] > entry : row.getColumns().entrySet()) {
              if (entry.getKey().length != Bytes.SIZEOF_LONG) {
                LOG.warn("Got entry where column length is longer than 8bytes. " +
                           "Expected only old format column enty. Got {} ", entry.getKey().length);
                continue;
              }
              long timeStamp = Bytes.toLong(entry.getKey());
              if (timeStamp > tillTime) {
                LOG.error("Got an entry where timestamp {} is less than tillTime {} searched for", timeStamp, tillTime);
                continue;
              }
              if (entriesDeleted < MAX_METADATA_ENTRIES) {
                table.delete(rowKey, entry.getKey());
                LOG.info("Deleted a entry in metadata table with timestamp {}", timeStamp);
              } else {
                // we don't want the transaction to timeout
                break;
              }
              entriesDeleted++;
            }
          }
        }
        return entriesDeleted;
      }
    });
  }

  /**
   * scans for meta data in old format which has expired the log retention.
   */
  public List<URI> scanAndGetFilesToDelete(final long tillTime) throws Exception {
    final List<URI> locationsToDelete = new ArrayList<>();

    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
        // we use the same tillTime for evetTimestamp and creationTimestamp for matching.
        byte[] endColumn = Bytes.add(Bytes.toBytes(VERSION), Bytes.toBytes(tillTime), Bytes.toBytes(tillTime));
        int expectedColumnLength = endColumn.length;
        // create range with tillTime as endColumn
        // todo make sure this doesn't include the new format columns, can we change version prefix to 2 instead?
        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(null, false, endColumn, true);
        Scan scan = new Scan(ROW_KEY_PREFIX, ROW_KEY_PREFIX_END, columnRangeFilter);
        int maxEntriesDeleted = 0;
        try (Scanner scanner = table.scan(scan)) {
          Row row;
          while ((row = scanner.next()) != null) {
            byte[] rowKey = row.getRow();
            for (Map.Entry<byte[], byte[] > entry : row.getColumns().entrySet()) {
              if (entry.getKey().length != expectedColumnLength) {
                LOG.warn("Got entry where column length is longer than {} bytes. " +
                           "Expected only new format column enty. Got {} bytes ",
                         expectedColumnLength, entry.getKey().length);
                continue;
              }
              long eventTimeStamp = Bytes.toLong(entry.getKey(), 1, Bytes.SIZEOF_LONG);
              long currentTimeStamp = Bytes.toLong(entry.getKey(), 9, Bytes.SIZEOF_LONG);
              if (eventTimeStamp > tillTime || currentTimeStamp > tillTime) {
                LOG.error("Got an entry with event timestamp {} and current ts {}," +
                            " greater than tillTime {} searched for", eventTimeStamp, currentTimeStamp, tillTime);
                continue;
              }
              if (maxEntriesDeleted < MAX_METADATA_ENTRIES) {
                locationsToDelete.add(new URI(Bytes.toString(entry.getValue())));
                table.delete(rowKey, entry.getKey());
              } else {
                // we don't want the transaction to timeout
                break;
              }
              maxEntriesDeleted++;
            }
          }
        }
      }
    });
    return locationsToDelete;
  }
}
