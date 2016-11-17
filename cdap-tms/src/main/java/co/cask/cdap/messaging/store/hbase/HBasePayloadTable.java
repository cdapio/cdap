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
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.store.AbstractPayloadTable;
import co.cask.cdap.messaging.store.DefaultPayloadTableEntry;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * HBase implementation of {@link PayloadTable}.
 */
public class HBasePayloadTable extends AbstractPayloadTable {
  private static final byte[] COL = Bytes.toBytes('c');

  private final HBaseTableUtil tableUtil;
  private final byte[] columnFamily;
  private final HTable hTable;

  private long writeTimestamp;
  private short pSeqId;

  public HBasePayloadTable(HBaseTableUtil tableUtil, HTable hTable, byte[] columnFamily) {
    this.tableUtil = tableUtil;
    this.hTable = hTable;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicId topicId, long transactionWritePointer, MessageId messageId,
                                        boolean inclusive, int limit) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_SHORT];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, transactionWritePointer);
    Bytes.putLong(startRow, topic.length + Bytes.SIZEOF_LONG, messageId.getWriteTimestamp());
    Bytes.putShort(startRow, topic.length + (2 * Bytes.SIZEOF_LONG), messageId.getPayloadSequenceId());
    if (!inclusive) {
      startRow = Bytes.incrementBytes(startRow, 1);
    }
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(topic))
      .setMaxResultSize(limit)
      .build();
    final ResultScanner scanner = hTable.getScanner(scan);

    return new AbstractCloseableIterator<Entry>() {
      private boolean closed = false;

      @Override
      public void close() {
        try {
          scanner.close();
        } finally {
          endOfData();
          closed = true;
        }
      }

      @Override
      protected Entry computeNext() {
        if (closed) {
          return endOfData();
        }

        Result result;
        try {
          result = scanner.next();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }

        if (result != null) {
          return new DefaultPayloadTableEntry(result.getRow(), result.getValue(columnFamily, COL));
        }
        return endOfData();
      }
    };
  }

  @Override
  public void store(Iterator<Entry> entries) throws IOException {
    long writeTs = System.currentTimeMillis();
    if (writeTs != writeTimestamp) {
      pSeqId = 0;
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
        rowKey = new byte[topic.length + (2 * Bytes.SIZEOF_LONG) + Bytes.SIZEOF_SHORT];
      }

      Bytes.putBytes(rowKey, 0, topic, 0, topic.length);
      Bytes.putLong(rowKey, topic.length, entry.getTransactionWritePointer());
      Bytes.putLong(rowKey, topic.length + Bytes.SIZEOF_LONG, writeTimestamp);
      Bytes.putShort(rowKey, topic.length + (2 * Bytes.SIZEOF_LONG), pSeqId++);

      Put put = tableUtil.buildPut(rowKey)
        .add(columnFamily, COL, entry.getPayload())
        .build();
      batchPuts.add(put);
    }
    if (!batchPuts.isEmpty()) {
      hTable.put(batchPuts);
      if (!hTable.isAutoFlush()) {
        hTable.flushCommits();
      }
    }
  }

  @Override
  protected void performDelete(byte[] startRow, byte[] stopRow) throws IOException {
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(stopRow)
      .build();

    List<Delete> batchDeletes = new ArrayList<>();
    try (ResultScanner scanner = hTable.getScanner(scan)) {
      for (Result result : scanner) {
        Delete delete = tableUtil.buildDelete(result.getRow())
          .build();
        batchDeletes.add(delete);
        hTable.delete(delete);
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
}
