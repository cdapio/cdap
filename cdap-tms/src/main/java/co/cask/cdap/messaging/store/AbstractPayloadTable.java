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

package co.cask.cdap.messaging.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.util.Iterator;

/**
 * Contains common logic for implementation of {@link PayloadTable}.
 */
public abstract class AbstractPayloadTable implements PayloadTable {

  /**
   * Store the {@link RawPayloadTableEntry}s persistently.
   *
   * @param tableEntries {@link Iterator} of {@link RawPayloadTableEntry}s
   * @throws IOException thrown if there was an error while storing the entries
   */
  protected abstract void persist(Iterator<RawPayloadTableEntry> tableEntries) throws IOException;

  /**
   * Read the {@link RawPayloadTableEntry}s given a key range.
   *
   * @param startRow start row prefix
   * @param stopRow stop row prefix
   * @param limit maximum number of messages to read
   * @return {@link CloseableIterator} of {@link RawPayloadTableEntry}s
   * @throws IOException thrown if there was an error while trying to read the entries from the table
   */
  protected abstract CloseableIterator<RawPayloadTableEntry> read(byte[] startRow, byte[] stopRow,
                                                                  int limit) throws IOException;

  @Override
  public void store(Iterator<? extends Entry> entries) throws IOException {
    persist(new StoreIterator(entries));
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, long transactionWritePointer,
                                        MessageId messageId, final boolean inclusive, int limit) throws IOException {
    byte[] topic = MessagingUtils.toDataKeyPrefix(metadata.getTopicId(), metadata.getGeneration());
    final byte[] startRow = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_SHORT];
    byte[] stopRow = new byte[topic.length + Bytes.SIZEOF_LONG];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putBytes(stopRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, transactionWritePointer);
    Bytes.putLong(stopRow, topic.length, transactionWritePointer);
    Bytes.putLong(startRow, topic.length + Bytes.SIZEOF_LONG, messageId.getPayloadWriteTimestamp());
    Bytes.putShort(startRow, topic.length + (2 * Bytes.SIZEOF_LONG), messageId.getPayloadSequenceId());
    stopRow = Bytes.stopKeyForPrefix(stopRow);

    final CloseableIterator<RawPayloadTableEntry> scanner = read(startRow, stopRow, limit);
    return new AbstractCloseableIterator<Entry>() {
      private boolean closed = false;
      private byte[] skipStartRow = inclusive ? null : startRow;

      @Override
      protected Entry computeNext() {
        if (closed || (!scanner.hasNext())) {
          return endOfData();
        }
        RawPayloadTableEntry entry = scanner.next();

        // See if we need to skip the first row returned by the scanner
        if (skipStartRow != null) {
          byte[] row = skipStartRow;
          // After first row, we don't need to match anymore
          skipStartRow = null;
          if (Bytes.equals(row, entry.getKey()) && !scanner.hasNext()) {
            return endOfData();
          }
          entry = scanner.next();
        }
        return new ImmutablePayloadTableEntry(entry.getKey(), entry.getValue());
      }

      @Override
      public void close() {
        try {
          scanner.close();
        } finally {
          endOfData();
          closed = true;
        }
      }
    };
  }

  /**
   * A resettable {@link Iterator} for iterating over {@link RawPayloadTableEntry} based on a given
   * iterator of {@link Entry}.
   */
  private static class StoreIterator extends AbstractIterator<RawPayloadTableEntry> {

    private final Iterator<? extends Entry> entries;
    private final RawPayloadTableEntry tableEntry;
    private TopicId topicId;
    private int generation;
    private byte[] topic;
    private byte[] rowKey;
    private Entry nextEntry;

    private StoreIterator(Iterator<? extends Entry> entries) {
      this.entries = entries;
      this.tableEntry = new RawPayloadTableEntry();
    }

    @Override
    protected RawPayloadTableEntry computeNext() {
      if (!entries.hasNext()) {
        return endOfData();
      }

      Entry entry = entries.next();
      if (topicId == null || (!topicId.equals(entry.getTopicId())) || (generation != entry.getGeneration())) {
        topicId = entry.getTopicId();
        generation = entry.getGeneration();
        topic = MessagingUtils.toDataKeyPrefix(topicId, entry.getGeneration());
        rowKey = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_SHORT];
      }

      Bytes.putBytes(rowKey, 0, topic, 0, topic.length);
      Bytes.putLong(rowKey, topic.length, entry.getTransactionWritePointer());
      Bytes.putLong(rowKey, topic.length + Bytes.SIZEOF_LONG, entry.getPayloadWriteTimestamp());
      Bytes.putShort(rowKey, topic.length + (2 * Bytes.SIZEOF_LONG), entry.getPayloadSequenceId());
      return tableEntry.set(rowKey, entry.getPayload());
    }
  }
}
