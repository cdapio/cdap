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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.logging.appender.system.LogPathIdentifier;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static co.cask.cdap.api.Transactionals.execute;

/**
 * class to read log meta data table
 */
public class FileMetaDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataReader.class);

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
  private final LocationFactory locationFactory;
  private final Impersonator impersonator;

  @Inject
  FileMetaDataReader(final DatasetFramework datasetFramework,
                     final TransactionSystemClient transactionSystemClient,
                     final LocationFactory locationFactory, final Impersonator impersonator) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), transactionSystemClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.locationFactory = locationFactory;
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
    List<LogLocation> endTimestampFilteredList = execute(transactional, context -> {
      Table table = LoggingStoreTableUtil.getMetadataTable(datasetFramework, context);
      List<LogLocation> files = new ArrayList<>();
      // get and add files in old format
      files.addAll(getFilesInOldFormat(table, logPathIdentifier, endTimestampMs));
      // get add files from new format
      files.addAll(getFilesInNewFormat(table, logPathIdentifier, endTimestampMs));
      return files;
    }, Exception.class);
    // performing extra filtering (based on start timestamp) outside the transaction
    return getFilesInRange(endTimestampFilteredList, startTimestampMs);
  }

  private List<LogLocation> getFilesInOldFormat(
    Table metaTable, LogPathIdentifier logPathIdentifier, long endTimestampMs) throws Exception {
    List<LogLocation> files = new ArrayList<>();
    final Row cols = metaTable.get(getOldRowKey(logPathIdentifier));
    for (final Map.Entry<byte[], byte[]> entry : cols.getColumns().entrySet()) {
      // old rowkey format length is 8 bytes (just the event timestamp is the column key)
      if (entry.getKey().length == 8) {
        long eventTimestamp = Bytes.toLong(entry.getKey());
        if (eventTimestamp <= endTimestampMs) {
          Location fileLocation =
            impersonator.doAs(new NamespaceId(logPathIdentifier.getNamespaceId()),
                              new Callable<Location>() {
                                @Override
                                public Location call() throws Exception {
                                  // we stored uri in old format
                                  return Locations.getLocationFromAbsolutePath(
                                    locationFactory, new URI(Bytes.toString(entry.getValue())).getPath());
                                }
                              });
          // old format
          files.add(new LogLocation(LogLocation.VERSION_0,
                                    eventTimestamp,
                                    // use 0 as current time as this information is not available
                                    0,
                                    fileLocation,
                                    logPathIdentifier.getNamespaceId(), impersonator));
        }
      } else {
        LOG.warn("For row-key {}, got column entry with unexpected key length {}",
                 logPathIdentifier.getOldRowkey(), entry.getKey().length);
      }
    }
    return files;
  }

  private List<LogLocation> getFilesInNewFormat(
    Table metaTable, LogPathIdentifier logPathIdentifier, long endTimestampMs) throws URISyntaxException {
    // create scanner with
    // start rowkey prefix:context:event-time(0):create-time(0)
    // end rowkey  prefix:context:event-time(endTimestamp):0(create-time doesn't matter for get files)
    // add these files to the list
    List<LogLocation> files = new ArrayList<>();

    byte[] logPathIdBytes = Bytes.toBytes(logPathIdentifier.getRowkey());
    byte[] startRowKey = Bytes.concat(LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX,
                                      logPathIdBytes,
                                      Bytes.toBytes(0L),
                                      Bytes.toBytes(0L));

    byte[] endRowKey = Bytes.concat(LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX,
                                    logPathIdBytes,
                                    // end row-key is exclusive, so use endTimestamp + 1
                                    Bytes.toBytes(endTimestampMs + 1),
                                    Bytes.toBytes(0L));
    int prefixLength = LoggingStoreTableUtil.NEW_FILE_META_ROW_KEY_PREFIX.length + logPathIdBytes.length;

    try (Scanner scanner = metaTable.scan(startRowKey, endRowKey)) {
      Row row;
      while ((row = scanner.next()) != null) {
        // column value is the file location
        byte[] value = row.get(LoggingStoreTableUtil.META_TABLE_COLUMN_KEY);
        files.add(new LogLocation(LogLocation.VERSION_1,
                                  Bytes.toLong(row.getRow(), prefixLength, Bytes.SIZEOF_LONG),
                                  Bytes.toLong(row.getRow(), prefixLength + Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG),
                                  // we store path in new format
                                  Locations.getLocationFromAbsolutePath(locationFactory, (Bytes.toString(value))),
                                  logPathIdentifier.getNamespaceId(), impersonator));

      }
    }
    return files;
  }

  private byte[] getOldRowKey(LogPathIdentifier logPathIdentifier) {
    return Bytes.add(LoggingStoreTableUtil.OLD_FILE_META_ROW_KEY_PREFIX, logPathIdentifier.getOldRowkey().getBytes());
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
