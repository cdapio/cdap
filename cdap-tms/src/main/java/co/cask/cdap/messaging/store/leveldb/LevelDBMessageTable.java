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

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.DefaultMessageTableEntry;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.proto.id.TopicId;
import org.apache.tephra.Transaction;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MessageTable}.
 */
public class LevelDBMessageTable implements MessageTable {
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);
  private static final String PAYLOAD_COL = "p";
  private static final String TX_COL = "t";

  private final DB levelDB;
  private long writeTimestamp;
  private short seqId;

  public LevelDBMessageTable(DB levelDB) {
    this.levelDB = levelDB;
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long startTime, int limit, @Nullable Transaction transaction)
    throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Long.BYTES];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, startTime);
    byte[] stopKey = Bytes.stopKeyForPrefix(topic);
    return new LevelDBCloseableIterator(levelDB, startRow, stopKey, limit, transaction);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, MessageId messageId, boolean inclusive, final int limit,
                                        @Nullable Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Long.BYTES + Short.BYTES];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, messageId.getPublishTimestamp());
    Bytes.putShort(startRow, topic.length + Long.BYTES, messageId.getSequenceId());
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    byte[] stopKey = Bytes.stopKeyForPrefix(topic);
    return new LevelDBCloseableIterator(levelDB, startRow, stopKey, limit, transaction);
  }

  @Override
  public void store(Iterator<Entry> entries) throws IOException {
    long writeTs = System.currentTimeMillis();
    if (writeTs != writeTimestamp) {
      seqId = 0;
    }
    writeTimestamp = writeTs;
    while (entries.hasNext()) {
      Entry entry = entries.next();
      byte[] topic = MessagingUtils.toRowKeyPrefix(entry.getTopicId());
      byte[] tableKey = new byte[topic.length + Long.BYTES + Short.BYTES];
      Bytes.putBytes(tableKey, 0, topic, 0, topic.length);
      Bytes.putLong(tableKey, topic.length, writeTimestamp);
      Bytes.putShort(tableKey, topic.length + Long.BYTES, seqId++);

      long txWritePtr = -1;
      byte[] payload = entry.getPayload();
      if (entry.isTransactional()) {
        txWritePtr = entry.getTransactionWritePointer();
      }
      levelDB.put(tableKey, encodeValue(txWritePtr, payload), WRITE_OPTIONS);
    }
  }

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] targetTxBytes = Bytes.toBytes(transactionWritePointer);
    byte[] rowPrefix = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] stopRow = Bytes.stopKeyForPrefix(rowPrefix);

    List<byte[]> rowKeysToDelete = new ArrayList<>();
    try (CloseableIterator<Map.Entry<byte[], byte[]>> rowIterator = new DBScanIterator(levelDB, rowPrefix, stopRow)) {
      while (rowIterator.hasNext()) {
        Map.Entry<byte[], byte[]> candidateRow = rowIterator.next();
        byte[] rowKey = candidateRow.getKey();
        Map<String, byte[]> columns = decodeValue(candidateRow.getValue());
        if (columns != null && columns.containsKey(TX_COL)) {
          byte[] txPtr = columns.get(TX_COL);
          if (Bytes.equals(txPtr, targetTxBytes)) {
            rowKeysToDelete.add(rowKey);
          }
        }
      }
    }

    for (byte[] deleteRowKey : rowKeysToDelete) {
      levelDB.delete(deleteRowKey);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }


  // Encoding:
  // If the returned byte array starts with 0, then it is a non-tx message and all the subsequent bytes are payload
  // If the returned byte array starts with 1, then next 8 bytes correspond to txWritePtr and rest are payload bytes
  private static byte[] encodeValue(long txWritePtr, @Nullable byte[] payload) {
    // Not transactional
    if (txWritePtr == -1) {
      payload = payload == null ? Bytes.EMPTY_BYTE_ARRAY : payload;
      return Bytes.add(new byte[] { 0 }, payload);
    }

    int resultSize = 1 + Long.BYTES;
    resultSize += (payload == null) ? 0 : payload.length;
    byte[] result = new byte[resultSize];
    result[0] = 1;
    Bytes.putLong(result, 1, txWritePtr);
    if (payload != null) {
      Bytes.putBytes(result, 1 + Long.BYTES, payload, 0, payload.length);
    }
    return result;
  }

  private static Map<String, byte[]> decodeValue(byte[] value) {
    Map<String, byte[]> data = new HashMap<>();
    if (value[0] == 0) {
      // just payload
      data.put(PAYLOAD_COL, Arrays.copyOfRange(value, 1, value.length));
    } else {
      data.put(TX_COL, Arrays.copyOfRange(value, 1, 1 + Long.BYTES));
      data.put(PAYLOAD_COL, Arrays.copyOfRange(value, 1 + Long.BYTES, value.length));
    }
    return data;
  }

  private static class LevelDBCloseableIterator extends AbstractCloseableIterator<Entry> {
    private final Transaction transaction;
    private int limit;
    private boolean closed = false;
    private final DBScanIterator iterator;

    LevelDBCloseableIterator(DB levelDB, byte[] startKey, byte[] stopKey, int limit,
                             @Nullable Transaction transaction) {
      this.iterator = new DBScanIterator(levelDB, startKey, stopKey);
      this.limit = limit;
      this.transaction = transaction;
    }

    @Override
    protected MessageTable.Entry computeNext() {
      if (closed) {
        return endOfData();
      }

      if (limit <= 0) {
        return endOfData();
      }

      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> row = iterator.next();
        Map<String, byte[]> columns = decodeValue(row.getValue());
        MessageTable.Entry entry = new DefaultMessageTableEntry(row.getKey(), columns.getOrDefault(PAYLOAD_COL, null),
                                                    columns.getOrDefault(TX_COL, null));
        if (validEntry(entry, transaction)) {
          limit--;
          return entry;
        }
      }
      return endOfData();
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

    private static boolean validEntry(MessageTable.Entry entry, Transaction transaction) {
      if (transaction == null || (!entry.isTransactional())) {
        return true;
      }
      long txWritePtr = entry.getTransactionWritePointer();
      return transaction.isVisible(txWritePtr);
    }
  }
}
