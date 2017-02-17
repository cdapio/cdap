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

package co.cask.cdap.logging.clean;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.logging.meta.LoggingStoreTableUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Class to scan and also delete meta data
 */
public class FileMetadataScanner {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetadataScanner.class);
  private static final byte[] OLD_ROW_KEY_PREFIX = LoggingStoreTableUtil.OLD_FILE_META_ROW_KEY_PREFIX;
  private static final byte[] OLD_ROW_KEY_PREFIX_END = Bytes.stopKeyForPrefix(OLD_ROW_KEY_PREFIX);

  private static final byte[] NEW_ROW_KEY_PREFIX = LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX;
  private static final byte[] NEW_ROW_KEY_PREFIX_END = Bytes.stopKeyForPrefix(NEW_ROW_KEY_PREFIX);

  // cut-off time discount from actual transaction timeout
  private static final int CUTOFF_DISCOUNT = 5;
  private final Transactional transactional;
  private final DatasetManager datasetManager;

  public FileMetadataScanner(DatasetManager datasetManager, Transactional transactional) {
    this.transactional = transactional;
    this.datasetManager = datasetManager;
  }

  /**
   * scans for meta data in old format which has expired the log retention.
   */
  @VisibleForTesting
  List<byte[]> scanAndDeleteOldMetaData(int transactionTimeout, final int cutoffTransactionTime) {
    final List<byte[]> deletedEntries = new ArrayList<>();
    try {
      transactional.execute(transactionTimeout, new TxRunnable() {
        public void run(DatasetContext context) throws Exception {
          Stopwatch stopwatch = new Stopwatch();
          stopwatch.start();
          Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
          // create range with tillTime as endColumn
          Scan scan = new Scan(OLD_ROW_KEY_PREFIX, OLD_ROW_KEY_PREFIX_END, null);
          try (Scanner scanner = table.scan(scan)) {
            Row row;
            while ((row = scanner.next()) != null) {
              if (stopwatch.elapsedTime(TimeUnit.SECONDS) > cutoffTransactionTime) {
                break;
              }
              byte[] rowKey = row.getRow();
              // delete all columns for this row
              table.delete(rowKey);
              deletedEntries.add(rowKey);
            }
          }
        }
      });
    } catch (TransactionFailureException e) {
      LOG.warn("Got Exception while deleting old metadata", e);
      // not required as we dont delete files in old format, but still safe to clear.
      deletedEntries.clear();
    }
    LOG.info("Deleted {} entries from the meta table for the old log format", deletedEntries.size());
    return deletedEntries;
  }

  /**
   * scans for meta data in old format which has expired the log retention.
   * @param tillTime time till which files will be deleted
   * @param transactionTimeout transaction timeout to use for scanning entries, deleting entries.
   * @return list of DeleteEntry - used to get files to delete for which metadata has already been deleted
   */
  public List<DeleteEntry> scanAndGetFilesToDelete(final long tillTime, final int transactionTimeout) {
    final List<DeleteEntry> toDelete = new ArrayList<>();
    if (transactionTimeout < CUTOFF_DISCOUNT) {
      LOG.warn("Transaction timeout for log cleanup is really low {}s", transactionTimeout);
    }
    final int cutOffTransactionTime =
      transactionTimeout > CUTOFF_DISCOUNT ? transactionTimeout - CUTOFF_DISCOUNT : transactionTimeout;
    try {
      transactional.execute(transactionTimeout, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          Stopwatch stopwatch = new Stopwatch();
          stopwatch.start();

          Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
          byte[] startRowKey = NEW_ROW_KEY_PREFIX;
          byte[] endRowKey = NEW_ROW_KEY_PREFIX_END;
          Scanner scanner;
          boolean reachedEnd = false;
          while (!reachedEnd) {
            if (stopwatch.elapsedTime(TimeUnit.SECONDS) > cutOffTransactionTime) {
              break;
            }
            scanner = table.scan(startRowKey, endRowKey);
            Row row;
            while ((row = scanner.next()) != null) {
              byte[] rowkey = row.getRow();
              // file creation time is the last 8-bytes in rowkey in the new format
              long creationTime = Bytes.toLong(rowkey, rowkey.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
              if (creationTime <= tillTime) {
                // expired - can be deleted
                toDelete.add(
                  new DeleteEntry(rowkey,
                                  new URI(Bytes.toString(row.get(LoggingStoreTableUtil.META_TABLE_COLUMN_KEY)))));
              } else {
                // update start-row key based on the logging context and start a new scan.
                startRowKey = Bytes.add(NEW_ROW_KEY_PREFIX, getNextContextStartKey(rowkey));
                scanner.close();
                break;
              }
            }
            // if row is null, then scanner next returned null. so we have reached the end.
            if (row == null) {
              reachedEnd = true;
              scanner.close();
            }
          }
        }
      });
    } catch (TransactionFailureException e) {
      LOG.warn("Got Exception while scanning metadata table", e);
      // if there is an exception, no metadata, so delete file should be skipped.
      return new ArrayList<>();
    }

    if (!toDelete.isEmpty()) {
      // we will call delete on old metadata even whenever there is expired entries to delete in new format.
      // though the first call will delete all old meta data.
      scanAndDeleteOldMetaData(transactionTimeout, cutOffTransactionTime);

      // delete meta data first
      return deleteNewMetadataEntries(toDelete, transactionTimeout, cutOffTransactionTime);
    }
    // toDelete is empty, safe to return that
    return toDelete;
  }


  class DeleteEntry {
    private byte[] rowkey;
    private URI locationIdentifier;

    private DeleteEntry(byte[] rowkey, URI locationIdentifier) {
      this.rowkey = rowkey;
      this.locationIdentifier = locationIdentifier;
    }

    private byte[] getRowKey() {
      return rowkey;
    }

    URI getLocationIdentifier() {
      return locationIdentifier;
    }
  }

  /**
   * delete the rows specified in the list
   * if delete time is closer to transaction timeout, we break and return list of deleted entries so far.
   * @param toDeleteRows
   * @return list of deleted entries.
   */
  private List<DeleteEntry> deleteNewMetadataEntries(final List<DeleteEntry> toDeleteRows,
                                                     int transactionTimeout, final int cutOffTransactionTimeout) {
    final List<DeleteEntry> deletedEntries = new ArrayList<>();
    try {
      transactional.execute(transactionTimeout, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          Stopwatch stopwatch = new Stopwatch();
          stopwatch.start();
          Table table = LoggingStoreTableUtil.getMetadataTable(context, datasetManager);
          for (DeleteEntry entry : toDeleteRows) {
            if (stopwatch.elapsedTime(TimeUnit.SECONDS) > cutOffTransactionTimeout) {
              break;
            }
            table.delete(entry.getRowKey());
            deletedEntries.add(entry);
          }
          stopwatch.stop();
          LOG.info("Deleted {} new metadata entries in {} ms", deletedEntries.size(), stopwatch.elapsedMillis());
        }
      });
    } catch (TransactionFailureException e) {
      LOG.warn("Exception while deleting metadata entries", e);
      // exception, no metadata entry deleted, skip deleting files
      return new ArrayList<>();
    }
    return deletedEntries;
  }

  private byte[] getNextContextStartKey(byte[] rowkey) {
    // rowkey : <prefix-bytes>:context:event-ts(8):creation-time(8)
    int contextLength = rowkey.length -
      (LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX.length + 2 * Bytes.SIZEOF_LONG);
    Preconditions.checkState(contextLength > 0, "Invalid row-key with length" + rowkey.length);
    byte[] context = new byte[contextLength];
    System.arraycopy(rowkey, LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX.length, context, 0, contextLength);
    return Bytes.stopKeyForPrefix(context);
  }

}
