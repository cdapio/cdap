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

package io.cdap.cdap.logging.clean;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Class to scan and also delete meta data
 */
public class FileMetadataCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetadataCleaner.class);
  private final TransactionRunner transactionRunner;

  public FileMetadataCleaner(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * scans for meta data in new format which has expired the log retention.
   * @param tillTime time till which files will be deleted
   * @param fileCleanupBatchSize transaction clean up batch size.
   * @return list of DeleteEntry - used to get files to delete for which metadata has already been deleted
   */
  public List<DeletedEntry> scanAndGetFilesToDelete(final long tillTime, final int fileCleanupBatchSize) {
    final List<DeletedEntry> toDelete = new ArrayList<>(fileCleanupBatchSize);

    try {
      final AtomicReference<Range> range = new AtomicReference<>(Range.all());
      // stop cleaning up if there are no files to delete or if we have reached batch size
      while (range.get() != null) {
        TransactionRunners.run(transactionRunner, context -> {
          StructuredTable table = context.getTable(StoreDefinition.LogFileMetaStore.LOG_FILE_META);
          range.set(scanFilesToDelete(table, fileCleanupBatchSize, tillTime, toDelete, range));
        }, IOException.class);
      }
    } catch (IOException e) {
      LOG.warn("Got Exception while scanning metadata table", e);
      // if there is an exception, no metadata, so delete file should be skipped.
      return new ArrayList<>();
    }

    if (!toDelete.isEmpty()) {
      // delete meta data entries in toDelete and get the file location list
      return deleteNewMetadataEntries(toDelete);
    }
    // toDelete is empty, safe to return that
    return toDelete;
  }

  @Nullable
  @SuppressWarnings("ConstantConditions")
  private Range scanFilesToDelete(StructuredTable table, int fileCleanupBatchSize, long tillTime,
                                  List<DeletedEntry> toDelete, AtomicReference<Range> range) throws IOException {
    try (CloseableIterator<StructuredRow> iter = table.scan(range.get(), fileCleanupBatchSize)) {
      while (iter.hasNext()) {
        if (toDelete.size() >= fileCleanupBatchSize) {
          return null;
        }
        StructuredRow row = iter.next();
        long creationTime = row.getLong(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD);
        if (creationTime <= tillTime) {
          // expired - can be deleted
          toDelete.add(new DeletedEntry(row.getString(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD),
                                        row.getLong(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD),
                                        row.getLong(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD),
                                        row.getString(StoreDefinition.LogFileMetaStore.FILE_FIELD)));
        } else {
          // return range to skip this logging context and move to next one.
          return Range.from(ImmutableList.of(
            Fields.stringField(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD,
                               row.getString(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD))),
                            Range.Bound.EXCLUSIVE);
        }
      }
      // if there are no more files to delete, return next range as null.
      return null;
    }
  }

  /**
   * delete the rows specified in the list
   * if delete time is closer to transaction timeout, we break and return list of deleted entries so far.
   * @param toDeleteRows
   * @return list of deleted entries.
   */
  private List<DeletedEntry> deleteNewMetadataEntries(final List<DeletedEntry> toDeleteRows) {
    final List<DeletedEntry> deletedEntries = new ArrayList<>();
    try {
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(StoreDefinition.LogFileMetaStore.LOG_FILE_META);

        for (DeletedEntry entry : toDeleteRows) {
          table.delete(getKeyFields(entry));
          deletedEntries.add(entry);
        }
        LOG.info("Deleted {} metadata entries.", deletedEntries.size());
      }, IOException.class);
    } catch (IOException e) {
      LOG.warn("Exception while deleting metadata entries", e);
      // exception, no metadata entry will be deleted, skip deleting files
      return new ArrayList<>();
    }
    return deletedEntries;
  }

  static final class DeletedEntry {
    private String identifier;
    private long eventTime;
    private long creationTime;
    private String location;

    private DeletedEntry(String identifier, long eventTime, long creationTime, String location) {
      this.identifier = identifier;
      this.eventTime = eventTime;
      this.creationTime = creationTime;
      this.location = location;
    }

    /**
     * identifier
     * @return identifier string
     */
    public String getIdentifier() {
      return identifier;
    }

    /**
     * event time
     * @return event time
     */
    public long getEventTime() {
      return eventTime;
    }

    /**
     * creation time of entry
     * @return creation time
     */
    public long getCreationTime() {
      return creationTime;
    }

    /**
     * path to be deleted
     * @return path string
     */
    String getPath() {
      return location;
    }
  }

  private List<Field<?>> getKeyFields(DeletedEntry entry) {
    return Arrays.asList(Fields.stringField(StoreDefinition.LogFileMetaStore.LOGGING_CONTEXT_FIELD,
                                            entry.getIdentifier()),
                         Fields.longField(StoreDefinition.LogFileMetaStore.EVENT_TIME_FIELD, entry.getEventTime()),
                         Fields.longField(StoreDefinition.LogFileMetaStore.CREATION_TIME_FIELD,
                                          entry.getCreationTime()));
  }
}
