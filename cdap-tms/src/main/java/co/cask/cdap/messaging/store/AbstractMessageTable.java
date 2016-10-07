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
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
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
   * Delete the transactionally published messages in the Table, given a key range and
   * the transaction write pointer byte array.
   *
   * @param startKey start row prefix
   * @param stopKey stop row prefix
   * @param targetTxBytes byte array representation of Transaction Write pointer
   * @throws IOException thrown if there was an error while trying to delete the entries
   */
  protected abstract void delete(byte[] startKey, byte[] stopKey, byte[] targetTxBytes) throws IOException;

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
    return new FetchIterator(scanner, limit, transaction);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, MessageId messageId, boolean inclusive, final int limit,
                                        @Nullable final Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, messageId.getPublishTimestamp());
    Bytes.putShort(startRow, topic.length + Bytes.SIZEOF_LONG, messageId.getSequenceId());
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    byte[] stopRow = Bytes.stopKeyForPrefix(topic);
    final CloseableIterator<RawMessageTableEntry> scanner = read(startRow, stopRow);
    return new FetchIterator(scanner, limit, transaction);
  }

  @Override
  public void store(Iterator<Entry> entries) throws IOException {
    persist(storeIterator.reset(entries));
  }

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] targetTxBytes = Bytes.toBytes(transactionWritePointer);
    byte[] startRow = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] stopRow = Bytes.stopKeyForPrefix(startRow);
    delete(startRow, stopRow, targetTxBytes);
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
    private boolean closed = false;
    private int maxLimit;

    FetchIterator(CloseableIterator<RawMessageTableEntry> scanner, int limit,
                  @Nullable Transaction transaction) {
      this.scanner = scanner;
      this.transaction = transaction;
      this.maxLimit = limit;
    }

    @Override
    protected Entry computeNext() {
      if (closed || (maxLimit <= 0)) {
        return endOfData();
      }

      while (scanner.hasNext()) {
        RawMessageTableEntry tableEntry = scanner.next();
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
   * An {@link Iterator} for storing {@link RawMessageTableEntry}.
   */
  private static class StoreIterator extends AbstractIterator<RawMessageTableEntry> {

    private final RawMessageTableEntry tableEntry = new RawMessageTableEntry();

    private Iterator<Entry> entries;
    private TopicId topicId;
    private byte[] topic;
    private byte[] rowKey;

    @Override
    protected RawMessageTableEntry computeNext() {
      if (entries.hasNext()) {
        Entry entry = entries.next();
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
      return endOfData();
    }

    private StoreIterator reset(Iterator<Entry> entries) {
      this.entries = entries;
      return this;
    }
  }
}
