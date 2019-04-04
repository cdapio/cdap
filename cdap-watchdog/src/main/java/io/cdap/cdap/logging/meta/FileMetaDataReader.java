/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.meta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.logging.appender.system.LogPathIdentifier;
import io.cdap.cdap.logging.write.LogLocation;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * class to read log meta data table
 */
public class FileMetaDataReader {
  private static final Comparator<LogLocation> LOG_LOCATION_COMPARATOR = (o1, o2) -> {
    int cmp = Longs.compare(o1.getEventTimeMs(), o2.getEventTimeMs());
    if (cmp != 0) {
      return cmp;
    }
    // when two log files have same timestamp, we order them by the file creation time
    return Longs.compare(o1.getFileCreationTimeMs(), o2.getFileCreationTimeMs());
  };

  private final TransactionRunner transactionRunner;
  private final LocationFactory locationFactory;
  private final Impersonator impersonator;

  @Inject
  FileMetaDataReader(TransactionRunner transactionRunner, LocationFactory locationFactory, Impersonator impersonator) {
    this.transactionRunner = transactionRunner;
    this.locationFactory = locationFactory;
    this.impersonator = impersonator;
  }

  /**
   * Returns a list of log files for a logging context.
   *
   * @param logPathIdentifier logging context identifier.
   * @param startTimestampMs  starting timestamp in milli seconds
   * @param endTimestampMs    ending timestamp in milli seconds
   * @return List of {@link LogLocation}
   */
  public List<LogLocation> listFiles(final LogPathIdentifier logPathIdentifier,
                                     final long startTimestampMs, final long endTimestampMs) throws IOException {
    List<LogLocation> endTimestampFilteredList = TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.LogFileMetaStore.LOG_FILE_META);
      // get add files from new format
      return getFiles(table, logPathIdentifier, endTimestampMs);
    }, IOException.class);

    // performing extra filtering (based on start timestamp) outside the transaction
    return getFilesInRange(endTimestampFilteredList, startTimestampMs);
  }

  private List<LogLocation> getFiles(StructuredTable metaTable, LogPathIdentifier logPathIdentifier,
                                     long endTimestampMs) throws IOException {
    // create scanner with
    // start rowkey prefix:context:event-time(0):create-time(0)
    // end rowkey  prefix:context:event-time(endTimestamp):0(create-time doesn't matter for get files)
    // add these files to the list
    List<LogLocation> files = new ArrayList<>();
    Range scanRange = Range.create(getKeyFields(logPathIdentifier.getRowkey(), 0L, 0L), Range.Bound.INCLUSIVE,
                                   getPartialKey(logPathIdentifier.getRowkey(), endTimestampMs),
                                   Range.Bound.INCLUSIVE);

    try (CloseableIterator<StructuredRow> iter = metaTable.scan(scanRange, Integer.MAX_VALUE)) {
      while (iter.hasNext()) {
        StructuredRow row = iter.next();
        files.add(fromRow(row, logPathIdentifier.getNamespaceId()));
      }
    }
    return files;
  }

  @SuppressWarnings("ConstantConditions")
  private LogLocation fromRow(StructuredRow row, String namespace) {
    return new LogLocation(LogLocation.VERSION_1,
                           row.getLong(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD),
                           row.getLong(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD),
                           Locations.getLocationFromAbsolutePath(
                             locationFactory, row.getString(StoreDefinition.LogFileMetaStore.FILE_FIELD)),
                           namespace, impersonator);
  }

  private List<LogLocation> getFilesInRange(List<LogLocation> files, long startTimeInMs) {
    // return if its empty
    if (files.isEmpty()) {
      return files;
    }
    // sort the list
    files.sort(LOG_LOCATION_COMPARATOR);

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

  @SuppressWarnings("SameParameterValue")
  private List<Field<?>> getKeyFields(String identifier, long eventTime, long currentTime) {
    return ImmutableList.of(Fields.stringField(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD, identifier),
                            Fields.longField(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD, eventTime),
                            Fields.longField(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD, currentTime));
  }

  private ImmutableList<Field<?>> getPartialKey(String identifier, long endTimestampMs) {
    return ImmutableList.of(
      Fields.stringField(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD, identifier),
      Fields.longField(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD, endTimestampMs));
  }
}
