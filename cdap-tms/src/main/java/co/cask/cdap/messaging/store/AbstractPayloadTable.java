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
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.TopicId;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Contains common logic for implementation of {@link PayloadTable}.
 */
public abstract class AbstractPayloadTable implements PayloadTable {

  private final StoreIterator storeIterator = new StoreIterator();

  /**
   * Delete the messages in the Table, given a key range.
   *
   * @param startRow start row prefix
   * @param stopRow stop row prefix
   * @throws IOException thrown if there was an error while trying to delete the entries
   */
  protected abstract void delete(byte[] startRow, byte[] stopRow) throws IOException;

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
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, transactionWritePointer);
    byte[] stopRow = Bytes.stopKeyForPrefix(startRow);
    delete(startRow, stopRow);
  }

  @Override
  public void store(Iterator<? extends Entry> entries) throws IOException {
    persist(storeIterator.reset(entries));
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long transactionWritePointer, MessageId messageId,
                                        final boolean inclusive, int limit) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
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
  private static class StoreIterator implements Iterator<RawPayloadTableEntry> {

    private final RawPayloadTableEntry tableEntry = new RawPayloadTableEntry();

    private Iterator<? extends Entry> entries;
    private TopicId topicId;
    private byte[] topic;
    private byte[] rowKey;
    private Entry nextEntry;

    @Override
    public boolean hasNext() {
      if (nextEntry != null) {
        return true;
      }
      if (!entries.hasNext()) {
        return false;
      }
      nextEntry = entries.next();
      return true;
    }

    @Override
    public RawPayloadTableEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Entry entry = nextEntry;
      nextEntry = null;

      if (topicId == null || (!topicId.equals(entry.getTopicId()))) {
        topicId = entry.getTopicId();
        topic = MessagingUtils.toRowKeyPrefix(topicId);
        rowKey = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_SHORT];
      }

      Bytes.putBytes(rowKey, 0, topic, 0, topic.length);
      Bytes.putLong(rowKey, topic.length, entry.getTransactionWritePointer());
      Bytes.putLong(rowKey, topic.length + Bytes.SIZEOF_LONG, entry.getPayloadWriteTimestamp());
      Bytes.putShort(rowKey, topic.length + (2 * Bytes.SIZEOF_LONG), entry.getPayloadSequenceId());
      return tableEntry.set(rowKey, entry.getPayload());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }

    private StoreIterator reset(Iterator<? extends Entry> entries) {
      this.entries = entries;
      this.nextEntry = null;
      return this;
    }
  }
}
