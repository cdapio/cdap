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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.PutBuilder;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.DefaultMessageTableEntry;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * HBase implementation of {@link MessageTable}.
 */
public class HBaseMessageTable implements MessageTable {
  private static final byte[] PAYLOAD_COL = Bytes.toBytes('p');
  private static final byte[] TX_COL = Bytes.toBytes('t');

  private final HBaseTableUtil tableUtil;
  private final byte[] columnFamily;
  private final HTable hTable;

  private long writeTimestamp;
  private short seqId;

  public HBaseMessageTable(HBaseTableUtil tableUtil, HTable hTable, byte[] columnFamily) {
    this.tableUtil = tableUtil;
    this.hTable = hTable;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long startTime, int limit,
                                        @Nullable Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Long.BYTES];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, startTime);
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(topic))
      .build();
    ResultScanner scanner = hTable.getScanner(scan);
    return new HBaseCloseableIterator(scanner, limit, columnFamily, transaction);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, MessageId messageId, boolean inclusive,
                                        int limit, @Nullable Transaction transaction) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Long.BYTES + Short.BYTES];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, messageId.getPublishTimestamp());
    Bytes.putShort(startRow, topic.length + Long.BYTES, messageId.getSequenceId());
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(topic))
      .build();
    ResultScanner scanner = hTable.getScanner(scan);
    return new HBaseCloseableIterator(scanner, limit, columnFamily, transaction);
  }

  @Override
  public void store(Iterator<Entry> entries) throws IOException {
    long writeTs = System.currentTimeMillis();
    if (writeTs != writeTimestamp) {
      seqId = 0;
    }
    writeTimestamp = writeTs;
    TopicId topicId = null;
    byte[] topic = null;
    byte[] rowKey = null;

    List<Put> batchPuts = new ArrayList<>();
    while (entries.hasNext()) {
      Entry entry = entries.next();
      // Create new byte arrays only when the topicId is different. Else, reuse the byte arrays.
      if (topicId == null || (!topicId.equals(entry.getTopicId()))) {
        topicId = entry.getTopicId();
        topic = MessagingUtils.toRowKeyPrefix(topicId);
        rowKey = new byte[topic.length + Long.BYTES + Short.BYTES];
      }

      Bytes.putBytes(rowKey, 0, topic, 0, topic.length);
      Bytes.putLong(rowKey, topic.length, writeTimestamp);
      Bytes.putShort(rowKey, topic.length + Long.BYTES, seqId++);

      PutBuilder putBuilder = tableUtil.buildPut(rowKey);
      if (entry.isTransactional()) {
        putBuilder.add(columnFamily, TX_COL, Bytes.toBytes(entry.getTransactionWritePointer()));
      }
      if (!entry.isPayloadReference()) {
        putBuilder.add(columnFamily, PAYLOAD_COL, entry.getPayload());
      }
      batchPuts.add(putBuilder.build());
    }

    if (!batchPuts.isEmpty()) {
      hTable.put(batchPuts);
      if (!hTable.isAutoFlush()) {
        hTable.flushCommits();
      }
    }
  }

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] rowPrefix = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] stopRow = Bytes.stopKeyForPrefix(rowPrefix);
    // TODO: Optimize this scan by passing in a messageId since this scan can be
    Scan scan = tableUtil.buildScan()
      .setStartRow(rowPrefix)
      .setStopRow(stopRow)
      .build();

    List<Delete> batchDeletes = new ArrayList<>();
    try (ResultScanner scanner = hTable.getScanner(scan)) {
      for (Result result : scanner) {
        if (result.containsColumn(columnFamily, TX_COL)) {
          byte[] txCol = result.getValue(columnFamily, TX_COL);
          if (Bytes.equals(txCol, Bytes.toBytes(transactionWritePointer))) {
            Delete delete = tableUtil.buildDelete(result.getRow())
              .build();
            batchDeletes.add(delete);
          }
        }
      }
    }

    if (!batchDeletes.isEmpty()) {
      hTable.delete(batchDeletes);
      if (!hTable.isAutoFlush()) {
        hTable.flushCommits();
      }
    }
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  private static class HBaseCloseableIterator extends AbstractCloseableIterator<Entry> {
    private final ResultScanner scanner;
    private final Transaction transaction;
    private final byte[] colFamily;
    private int limit;
    private boolean closed = false;

    HBaseCloseableIterator(ResultScanner scanner, int limit, byte[] colFamily, @Nullable Transaction transaction) {
      this.scanner = scanner;
      this.transaction = transaction;
      this.colFamily = colFamily;
      this.limit = limit;
    }

    @Override
    protected Entry computeNext() {
      if (closed) {
        return endOfData();
      }

      if (limit <= 0) {
        return endOfData();
      }

      Result result;
      try {
        result = scanner.next();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      if (result != null) {
        MessageTable.Entry entry = new DefaultMessageTableEntry(
          result.getRow(), result.getValue(colFamily, PAYLOAD_COL), result.getValue(colFamily, TX_COL));
        // If it is a valid entry, return it. Else close the iterator (to guarantee ordering).
        if (isVisible(entry, transaction)) {
          limit--;
          return entry;
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

    private boolean isVisible(MessageTable.Entry entry, @Nullable Transaction transaction) {
      if (transaction == null || (!entry.isTransactional())) {
        return true;
      }
      long txWritePtr = entry.getTransactionWritePointer();
      return transaction.isVisible(txWritePtr);
    }
  }
}
