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

package co.cask.cdap.logging.meta;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.logging.framework.LogPathIdentifier;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * class to read log meta data table
 */
public class FileMetaDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataReader.class);

  private static final byte[] ROW_KEY_PREFIX = LoggingStoreTableUtil.FILE_META_ROW_KEY_PREFIX;
  private static final Comparator<LogLocation> LOG_LOCATION_COMPARATOR = new Comparator<LogLocation>() {
    @Override
    public int compare(LogLocation o1, LogLocation o2) {
      int cmp = Longs.compare(o1.getEventTimeMs(), o2.getEventTimeMs());
      if (cmp != 0) {
        return cmp;
      }
      // when two log files have same timestamp, we order them by the file creation time
      return Longs.compare(o1.getFileCreationTimeMs(), o2.getFileCreationTimeMs());
    }
  };

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final RootLocationFactory rootLocationFactory;
  private final Impersonator impersonator;

  // Note: For the old log files reading.
  // The FileMetaDataReader needs to have a RootLocationFactory because for custom mapped namespaces the
  // location mapped to a namespace are from root of the filesystem. The FileMetaDataReader stores a location in
  // bytes to a hbase table and to construct it back to a Location it needs to work with a root based location factory.
  @Inject
  FileMetaDataReader(final DatasetFramework datasetFramework,
                     final TransactionSystemClient transactionSystemClient,
                     final RootLocationFactory rootLocationFactory, final Impersonator impersonator) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), transactionSystemClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.rootLocationFactory = rootLocationFactory;
    this.impersonator = impersonator;
  }
  /**
   * Returns a list of log files for a logging context.
   *
   * @param logPathIdentifier logging context identifier.
   * @param startTimestampMs starting timestamp in milli seconds
   * @param endTimestampMs ending timestamp in milli seconds
   * @return List of {@link LogLocation}
   */
  public List<LogLocation> listFiles(final LogPathIdentifier logPathIdentifier,
                                     final long startTimestampMs, final long endTimestampMs) throws Exception {
    try {
      List<LogLocation> endTimestampFilteredList =
        Transactions.execute(transactional, new TxCallable<List<LogLocation>>() {
          @Override
          public List<LogLocation> call(DatasetContext context) throws Exception {
            Table table = LoggingStoreTableUtil.getMetadataTable(datasetFramework, context);
            final Row cols = table.get(getRowKey(logPathIdentifier));

            if (cols.isEmpty()) {
              return Collections.EMPTY_LIST;
            }
            List<LogLocation> files = new ArrayList<>();
            for (Map.Entry<byte[], byte[]> entry : cols.getColumns().entrySet()) {
              // old rowkey format length is 8 bytes (just the event timestamp is the column key)
              // new row-key format length is 17 bytes (version_byte, event timestamp (8b) and current timestamp (8b)
              if (entry.getKey().length == 8) {
                long eventTimestamp = Bytes.toLong(entry.getKey());
                if (eventTimestamp <= endTimestampMs) {
                  // old format
                  files.add(new LogLocation(LogLocation.VERSION_0,
                                            eventTimestamp,
                                            // use 0 as current time as this information is not available
                                            0,
                                            rootLocationFactory.create(new URI(Bytes.toString(entry.getValue()))),
                                            logPathIdentifier.getNamespaceId(), impersonator));
                }
              } else if (entry.getKey().length == 17) {
                // skip the first (version) byte
                long eventTimestamp = Bytes.toLong(entry.getKey(), 1, Bytes.SIZEOF_LONG);
                if (eventTimestamp <= endTimestampMs) {
                  // new format
                  files.add(new LogLocation(LogLocation.VERSION_1,
                                            eventTimestamp,
                                            Bytes.toLong(entry.getKey(), 9, Bytes.SIZEOF_LONG),
                                            rootLocationFactory.create(new URI(Bytes.toString(entry.getValue()))),
                                            logPathIdentifier.getNamespaceId(), impersonator));
                }
              } else {
                LOG.warn("For row-key {}, got column entry with unexpected key length {}",
                         logPathIdentifier.getRowKey(), entry.getKey().length);
              }
            }
            return files;
          }
        });

      // performing extra filtering (based on start timestamp) outside the transaction
      return getFilesInRange(endTimestampFilteredList, startTimestampMs);
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e, Exception.class);
    }
  }

  private byte[] getRowKey(LogPathIdentifier logPathIdentifier) {
    return Bytes.add(ROW_KEY_PREFIX, logPathIdentifier.getRowKey().getBytes());
  }

  @VisibleForTesting
  List<LogLocation> getFilesInRange(List<LogLocation> files, long startTimeInMs) {
    // return if its empty
    if (files.isEmpty()) {
      return files;
    }
    // sort the list
    Collections.sort(files, LOG_LOCATION_COMPARATOR);

    // iterate the list from the end
    // we continue when the start timestamp of the log file is higher than the startTimeInMs
    // when we reach a file where start time is lower than the startTimeInMs we return the list from this index.
    // as the files with same startTimeInMs is sorted by creation timestamp,
    // we will return the file with most recent creation time - which is the expected behavior.
    // if we reach the beginning of the list, we return the entire list.
    List<LogLocation> filteredList = new ArrayList<>();
    for (LogLocation logLocation : Lists.reverse(files)) {
      long eventTimestamp = logLocation.getEventTimeMs();
      filteredList.add(0, logLocation);
      if (eventTimestamp < startTimeInMs) {
        break;
      }
    }
    return filteredList;
  }
}
