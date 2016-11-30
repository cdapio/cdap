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
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * Contains common logic for implementation of {@link MessageTable}.
 */
public abstract class AbstractMessageTable implements MessageTable {

  private final StoreIterator storeIterator = new StoreIterator();

  private enum Result {
    ACCEPT,
    SKIP,
    HOLD
  }

  /**
   * Store the {@link RawMessageTableEntry}s persistently.
   *
   * @param entries {@link Iterator} of {@link RawMessageTableEntry}s
   * @throws IOException thrown if there was an error while storing the entries
   */
  protected abstract void persist(Iterator<RawMessageTableEntry> entries) throws IOException;

  /**
   * Delete the transactionally published messages in the Table in the given key range.
   *
   * @param startKey start row to delete (inclusive)
   * @param stopKey stop row to delete (exclusive)
   * @throws IOException thrown if there was an error while trying to delete the entries
   */
  protected abstract void delete(byte[] startKey, byte[] stopKey) throws IOException;

  /**
   * Read the {@link RawMessageTableEntry}s given a key range.
   *
   * @param startRow start row prefix
   * @param stopRow stop row prefix
   * @return {@link CloseableIterator} of {@link RawMessageTableEntry}s
   * @throws IOException throw if there was an error while trying to read the entries from the table
   */
  protected abstract CloseableIterator<RawMessageTableEntry> read(byte[] startRow, byte[] stopRow) throws IOException;

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long startTime, int limit,
                                        @Nullable Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, startTime);
    byte[] stopRow = Bytes.stopKeyForPrefix(topic);
    final CloseableIterator<RawMessageTableEntry> scanner = read(startRow, stopRow);
    return new FetchIterator(scanner, limit, null, transaction);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, MessageId messageId, boolean inclusive, final int limit,
                                        @Nullable final Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, messageId.getPublishTimestamp());
    Bytes.putShort(startRow, topic.length + Bytes.SIZEOF_LONG, messageId.getSequenceId());
    byte[] stopRow = Bytes.stopKeyForPrefix(topic);
    final CloseableIterator<RawMessageTableEntry> scanner = read(startRow, stopRow);
    return new FetchIterator(scanner, limit, inclusive ? null : startRow, transaction);
  }

  @Override
  public void store(Iterator<? extends Entry> entries) throws IOException {
    persist(storeIterator.reset(entries));
  }

  @Override
  public void delete(TopicId topicId, long startTimestamp, short startSequenceId,
                     long endTimestamp, short endSequenceId) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, startTimestamp);
    Bytes.putShort(startRow, topic.length + Bytes.SIZEOF_LONG, startSequenceId);

    byte[] stopRow = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(stopRow, 0, topic, 0, topic.length);
    Bytes.putLong(stopRow, topic.length, endTimestamp);
    Bytes.putShort(stopRow, topic.length + Bytes.SIZEOF_LONG, endSequenceId);

    delete(startRow, Bytes.stopKeyForPrefix(stopRow));
  }

  private static Result isVisible(@Nullable byte[] txPtr, @Nullable Transaction transaction) {
    // No transaction info available, so accept this message (it must have been published non-transactionally)
    if (transaction == null || txPtr == null) {
      return Result.ACCEPT;
    }

    long txWritePtr = Bytes.toLong(txPtr);
    // This transaction is visible, hence accept the message
    if (transaction.isVisible(txWritePtr)) {
      return Result.ACCEPT;
    }

    // This transaction is an invalid transaction, so skip the entry and proceed to the next
    if (Arrays.binarySearch(transaction.getInvalids(), txWritePtr) >= 0) {
      return Result.SKIP;
    }

    // This transaction has not yet been committed, hence hold to ensure ordering
    return Result.HOLD;
  }

  /**
   * An {@link Iterator} for fetching {@link Entry} from the the message table.
   */
  private static class FetchIterator extends AbstractCloseableIterator<Entry> {
    private final CloseableIterator<RawMessageTableEntry> scanner;
    private final Transaction transaction;
    private byte[] skipStartRow;
    private boolean closed = false;
    private int maxLimit;

    FetchIterator(CloseableIterator<RawMessageTableEntry> scanner, int limit, @Nullable byte[] skipStartRow,
                  @Nullable Transaction transaction) {
      this.scanner = scanner;
      this.transaction = transaction;
      this.skipStartRow = skipStartRow;
      this.maxLimit = limit;
    }

    @Override
    protected Entry computeNext() {
      if (closed || (maxLimit <= 0)) {
        return endOfData();
      }

      while (scanner.hasNext()) {
        RawMessageTableEntry tableEntry = scanner.next();

        // See if we need to skip the first row returned by the scanner
        if (skipStartRow != null) {
          byte[] row = skipStartRow;
          // After first row, we don't need to match anymore
          skipStartRow = null;
           if (Bytes.equals(row, tableEntry.getKey())) {
             continue;
           }
        }
        Result status = isVisible(tableEntry.getTxPtr(), transaction);
        if (status == Result.ACCEPT) {
          maxLimit--;
          return new ImmutableMessageTableEntry(tableEntry.getKey(), tableEntry.getPayload(), tableEntry.getTxPtr());
        }

        if (status == Result.HOLD) {
          break;
        }
      }
      return endOfData();
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
  }

  /**
   * A resettable {@link Iterator} for iterating over {@link RawMessageTableEntry} based on a given
   * iterator of {@link Entry}.
   */
  private static class StoreIterator implements Iterator<RawMessageTableEntry> {

    private final RawMessageTableEntry tableEntry = new RawMessageTableEntry();

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
    public RawMessageTableEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Entry entry = nextEntry;
      nextEntry = null;
      // Create new byte arrays only when the topicId is different. Else, reuse the byte arrays.
      if (topicId == null || (!topicId.equals(entry.getTopicId()))) {
        topicId = entry.getTopicId();
        topic = MessagingUtils.toRowKeyPrefix(topicId);
        rowKey = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
      }

      Bytes.putBytes(rowKey, 0, topic, 0, topic.length);
      Bytes.putLong(rowKey, topic.length, entry.getPublishTimestamp());
      Bytes.putShort(rowKey, topic.length + Bytes.SIZEOF_LONG, entry.getSequenceId());

      byte[] txPtr = null;
      if (entry.isTransactional()) {
        txPtr = Bytes.toBytes(entry.getTransactionWritePointer());
      }
      return tableEntry.set(rowKey, txPtr, entry.getPayload());
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
