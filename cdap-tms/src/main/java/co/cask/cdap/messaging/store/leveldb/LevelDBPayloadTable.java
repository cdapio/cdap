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
import co.cask.cdap.messaging.store.AbstractPayloadTable;
import co.cask.cdap.messaging.store.DefaultPayloadTableEntry;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.proto.id.TopicId;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * LevelDB implementation of {@link PayloadTable}.
 */
public class LevelDBPayloadTable extends AbstractPayloadTable {
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);

  private final DB levelDB;

  private long writeTimestamp;
  private short pSeqId;

  public LevelDBPayloadTable(DB levelDB) {
    this.levelDB = levelDB;
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long transactionWriterPointer, MessageId messageId,
                                        boolean inclusive, final int limit)
    throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startKey = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Short.SIZE];
    Bytes.putBytes(startKey, 0, topic, 0, topic.length);
    Bytes.putLong(startKey, topic.length, transactionWriterPointer);
    Bytes.putLong(startKey, topic.length + Bytes.SIZEOF_LONG, messageId.getWriteTimestamp());
    Bytes.putShort(startKey, topic.length + (2 * Bytes.SIZEOF_LONG), messageId.getPayloadSequenceId());
    if (!inclusive) {
      startKey = Bytes.incrementBytes(startKey, 1);
    }
    byte[] stopKey = Bytes.stopKeyForPrefix(topic);
    return new LevelDBCloseableIterator(levelDB, startKey, stopKey, limit);
  }

  @Override
  public void store(Iterator<Entry> entries) throws IOException {
    long writeTs = System.currentTimeMillis();
    if (writeTs != writeTimestamp) {
      pSeqId = 0;
    }
    writeTimestamp = writeTs;
    try {
      while (entries.hasNext()) {
        Entry entry = entries.next();
        byte[] topic = MessagingUtils.toRowKeyPrefix(entry.getTopicId());
        byte[] tableKeyBytes = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_SHORT];
        Bytes.putBytes(tableKeyBytes, 0, topic, 0, topic.length);
        Bytes.putLong(tableKeyBytes, topic.length, entry.getTransactionWritePointer());
        Bytes.putLong(tableKeyBytes, topic.length + Bytes.SIZEOF_LONG, writeTimestamp);
        Bytes.putShort(tableKeyBytes, topic.length + (2 * Bytes.SIZEOF_LONG), pSeqId++);
        levelDB.put(tableKeyBytes, entry.getPayload(), WRITE_OPTIONS);
      }
    } catch (DBException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  protected void performDelete(byte[] startRow, byte[] stopRow) throws IOException {
    List<byte[]> rowKeysToDelete = new ArrayList<>();
    try (CloseableIterator<Map.Entry<byte[], byte[]>> rowIterator = new DBScanIterator(levelDB, startRow, stopRow)) {
      while (rowIterator.hasNext()) {
        Map.Entry<byte[], byte[]> candidateRow = rowIterator.next();
        rowKeysToDelete.add(candidateRow.getKey());
      }
    }

    try {
      for (byte[] deleteRowKey : rowKeysToDelete) {
        levelDB.delete(deleteRowKey);
      }
    } catch (DBException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  private static class LevelDBCloseableIterator extends AbstractCloseableIterator<Entry> {
    private final DBScanIterator iterator;
    private int limit;
    private boolean closed = false;

    LevelDBCloseableIterator(DB levelDB, byte[] startKey, byte[] stopKey, int limit) {
      this.iterator = new DBScanIterator(levelDB, startKey, stopKey);
      this.limit = limit;
    }

    @Override
    protected Entry computeNext() {
      if (closed) {
        return endOfData();
      }

      if (limit <= 0 || (!iterator.hasNext())) {
        return endOfData();
      }

      Map.Entry<byte[], byte[]> row = iterator.next();
      limit--;
      return new DefaultPayloadTableEntry(row.getKey(), row.getValue());
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
  }
}
