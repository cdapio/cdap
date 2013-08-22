/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.collect.Lists;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;

import java.util.List;

/**
 *
 */
public final class LevelDBQueue2Producer extends AbstractQueue2Producer {

  private final DB db;
  private final List<byte[]> rollbackKeys;
  private final byte[] queueRowPrefix;

  public LevelDBQueue2Producer(DB db, QueueName queueName, QueueMetrics queueMetrics) {
    super(queueMetrics);
    this.db = db;
    this.rollbackKeys = Lists.newArrayList();
    this.queueRowPrefix = QueueUtils.getQueueRowPrefix(queueName);
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    rollbackKeys.clear();
  }

  @Override
  protected void persist(Iterable<QueueEntry> entries, Transaction transaction) throws Exception {
    long writePointer = transaction.getWritePointer();
    byte[] rowKeyPrefix = Bytes.add(queueRowPrefix, Bytes.toBytes(writePointer));
    int count = 0;
    WriteBatch writeBatch = db.createWriteBatch();

    for (QueueEntry entry : entries) {
      // Row key = queue_name + writePointer + counter
      byte[] rowKey = Bytes.add(rowKeyPrefix, Bytes.toBytes(count++));
      rollbackKeys.add(rowKey);

      KeyValue dataKV = new KeyValue(rowKey, QueueConstants.COLUMN_FAMILY, QueueConstants.DATA_COLUMN,
                                     KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Put, entry.getData());
      KeyValue metaKV = new KeyValue(rowKey, QueueConstants.COLUMN_FAMILY, QueueConstants.META_COLUMN,
                                     KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Put,
                                     QueueEntry.serializeHashKeys(entry.getHashKeys()));
      writeBatch.put(dataKV.getKey(), dataKV.getValue());
      writeBatch.put(metaKV.getKey(), metaKV.getValue());
    }
    db.write(writeBatch);
  }

  @Override
  protected void doRollback(Transaction transaction) throws Exception {
    if (rollbackKeys.isEmpty()) {
      return;
    }

    WriteBatch writeBatch = db.createWriteBatch();
    for (byte[] rowKey : rollbackKeys) {
      KeyValue dataKV = new KeyValue(rowKey, QueueConstants.COLUMN_FAMILY, QueueConstants.DATA_COLUMN,
                                     KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Put);
      KeyValue metaKV = new KeyValue(rowKey, QueueConstants.COLUMN_FAMILY, QueueConstants.META_COLUMN,
                                     KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Put);

      writeBatch.delete(dataKV.getKey());
      writeBatch.delete(metaKV.getKey());
    }

    db.write(writeBatch);
  }
}
