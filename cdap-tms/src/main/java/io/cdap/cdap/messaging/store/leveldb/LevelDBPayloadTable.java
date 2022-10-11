/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.leveldb;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.AbstractPayloadTable;
import io.cdap.cdap.messaging.store.ImmutablePayloadTableEntry;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.messaging.store.RawPayloadTableEntry;
import io.cdap.cdap.proto.id.TopicId;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * LevelDB implementation of {@link PayloadTable}.
 */
public class LevelDBPayloadTable extends AbstractPayloadTable {
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);
  private final DB levelDB;
  private final TopicMetadata topicMetadata;

  LevelDBPayloadTable(DB levelDB, TopicMetadata topicMetadata) {
    this.levelDB = levelDB;
    this.topicMetadata = topicMetadata;
  }

  private void checkTopic(TopicId topicId, int generation) {
    Preconditions.checkArgument(this.topicMetadata.getTopicId().equals(topicId), "Not allowed to use table with a " +
      "different topic id. Table's topic Id: {}. Specified topic id: {}", this.topicMetadata.getTopicId(), topicId);
    Preconditions.checkArgument(this.topicMetadata.getGeneration() == generation, "Not allowed to use table with " +
                                  "a different generation id. Table's generation: {}. Specified generation: {}",
                                this.topicMetadata.getGeneration(), generation);
  }

  @Override
  protected CloseableIterator<RawPayloadTableEntry> read(byte[] startRow, byte[] stopRow,
                                                         final int limit) throws IOException {
    final DBScanIterator iterator = new DBScanIterator(levelDB, startRow, stopRow);
    return new AbstractCloseableIterator<RawPayloadTableEntry>() {
      private final RawPayloadTableEntry tableEntry = new RawPayloadTableEntry();
      private boolean closed;
      private int maxLimit = limit;

      @Override
      protected RawPayloadTableEntry computeNext() {
        if (closed || maxLimit <= 0 || (!iterator.hasNext())) {
          return endOfData();
        }

        Map.Entry<byte[], byte[]> row = iterator.next();
        maxLimit--;
        return tableEntry.set(row.getKey(), row.getValue());
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } finally {
          endOfData();
          closed = true;
        }
      }
    };
  }

  @Override
  public void persist(Iterator<RawPayloadTableEntry> entries) throws IOException {
    try (WriteBatch writeBatch = levelDB.createWriteBatch()) {
      while (entries.hasNext()) {
        RawPayloadTableEntry entry = entries.next();
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        // LevelDB doesn't make copies, and since we reuse RawPayloadTableEntry object, we need to create copies.
        writeBatch.put(Arrays.copyOf(key, key.length), Arrays.copyOf(value, value.length));
      }
      levelDB.write(writeBatch, WRITE_OPTIONS);
    } catch (DBException ex) {
      throw new IOException(ex);
    }
  }

  /**
   * Delete messages of a {@link TopicId} that has exceeded the TTL or if it belongs to an older generation
   *
   * @param currentTime current timestamp
   * @throws IOException error occurred while trying to delete a row in LevelDB
   */
  void pruneMessages(long currentTime) throws IOException {
    WriteBatch writeBatch = levelDB.createWriteBatch();
    long ttlInMs = TimeUnit.SECONDS.toMillis(topicMetadata.getTTL());
    byte[] startRow = MessagingUtils.toDataKeyPrefix(new TopicId(topicMetadata.getTopicId()),
                                                     Integer.parseInt(MessagingUtils.Constants.DEFAULT_GENERATION));
    byte[] stopRow = Bytes.stopKeyForPrefix(startRow);

    try (CloseableIterator<Map.Entry<byte[], byte[]>> rowIterator = new DBScanIterator(levelDB, startRow, stopRow)) {
      while (rowIterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = rowIterator.next();
        PayloadTable.Entry payloadTableEntry = new ImmutablePayloadTableEntry(entry.getKey(), entry.getValue());

        int dataGeneration = payloadTableEntry.getGeneration();
        int currGeneration = topicMetadata.getGeneration();
        checkTopic(new TopicId(topicMetadata.getTopicId()), topicMetadata.getGeneration());
        if (MessagingUtils.isOlderGeneration(dataGeneration, currGeneration)) {
          writeBatch.delete(entry.getKey());
          continue;
        }

        if ((dataGeneration == Math.abs(currGeneration)) &&
          ((currentTime - payloadTableEntry.getPayloadWriteTimestamp()) > ttlInMs)) {
          writeBatch.delete(entry.getKey());
        } else {
          // terminate scanning table once an entry with write time after TTL is found, to avoid scanning whole table,
          // since the entries are sorted by time.
          break;
        }
      }
    }

    try {
      levelDB.write(writeBatch, WRITE_OPTIONS);
    } catch (DBException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void close() {
    // This method has to be an no-op instead of closing the underlying LevelDB object
    // This is because a given LevelDB object instance is shared within the same JVM
  }
}
