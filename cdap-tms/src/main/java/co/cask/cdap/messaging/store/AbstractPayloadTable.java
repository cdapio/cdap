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
import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.util.Iterator;

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
  public void store(Iterator<Entry> entries) throws IOException {
    persist(storeIterator.reset(entries));
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long transactionWritePointer, MessageId messageId,
                                        boolean inclusive, int limit) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_SHORT];
    byte[] stopRow = new byte[topic.length + Bytes.SIZEOF_LONG];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putBytes(stopRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, transactionWritePointer);
    Bytes.putLong(stopRow, topic.length, transactionWritePointer);
    Bytes.putLong(startRow, topic.length + Bytes.SIZEOF_LONG, messageId.getPayloadWriteTimestamp());
    Bytes.putShort(startRow, topic.length + (2 * Bytes.SIZEOF_LONG), messageId.getPayloadSequenceId());
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    stopRow = Bytes.stopKeyForPrefix(stopRow);

    final CloseableIterator<RawPayloadTableEntry> scanner = read(startRow, stopRow, limit);
    return new AbstractCloseableIterator<Entry>() {
      private boolean closed = false;

      @Override
      protected Entry computeNext() {
        if (closed || (!scanner.hasNext())) {
          return endOfData();
        }
        RawPayloadTableEntry entry = scanner.next();
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
   * An {@link Iterator} for storing {@link RawPayloadTableEntry} to the Payload Table.
   */
  private static class StoreIterator extends AbstractIterator<RawPayloadTableEntry> {

    private final RawPayloadTableEntry tableEntry = new RawPayloadTableEntry();

    private Iterator<Entry> entries;
    private TopicId topicId;
    private byte[] topic;
    private byte[] rowKey;

    @Override
    protected RawPayloadTableEntry computeNext() {
      if (entries.hasNext()) {
        Entry entry = entries.next();
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
      return endOfData();
    }

    private StoreIterator reset(Iterator<Entry> entries) {
      this.entries = entries;
      return this;
    }
  }
}
