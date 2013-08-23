/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.ImmutableList;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public final class LevelDBQueue2Consumer implements Queue2Consumer, TransactionAware {

  private final DB db;
  private final QueueName queueName;
  private final ConsumerConfig consumerConfig;
  private Transaction transaction;
  private byte[] queueRowPrefix;
  private byte[] startRow;

  public LevelDBQueue2Consumer(DB db, QueueName queueName, ConsumerConfig consumerConfig) {
    this.db = db;
    this.queueName = queueName;
    this.consumerConfig = consumerConfig;
    this.queueRowPrefix = QueueUtils.getQueueRowPrefix(queueName);
    this.startRow = queueRowPrefix;
  }

  @Override
  public QueueName getQueueName() {
    return queueName;
  }

  @Override
  public ConsumerConfig getConfig() {
    return consumerConfig;
  }

  @Override
  public DequeueResult dequeue() throws IOException {
    return dequeue(1);
  }

  @Override
  public DequeueResult dequeue(int maxBatchSize) throws IOException {
    DBIterator iterator = db.iterator();
    KeyValue startKV = new KeyValue(startRow, QueueConstants.COLUMN_FAMILY, null,
                               KeyValue.LATEST_TIMESTAMP, KeyValue.Type.Put);

    iterator.seek(startKV.getKey());
    while (iterator.hasNext()) {
      KeyValue keyValue = createKeyValue(iterator.next());

      System.out.println(Bytes.toString(keyValue.getQualifier()));
      System.out.println(keyValue.getValueLength());
    }

    return null;
  }

  @Override
  public void startTx(Transaction tx) {
    transaction = tx;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return ImmutableList.of();
  }

  @Override
  public boolean commitTx() throws Exception {
    return true;
  }

  @Override
  public void postTxCommit() {
    // No-op
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return true;
  }

  private KeyValue createKeyValue(Map.Entry<byte[], byte[]> entry) {
    byte[] key = entry.getKey();
    byte[] value = entry.getValue();

    int len = key.length + value.length + (2 * Bytes.SIZEOF_INT);
    byte[] kvBytes = new byte[len];
    int pos = 0;
    pos = Bytes.putInt(kvBytes, pos, key.length);
    pos = Bytes.putInt(kvBytes, pos, value.length);
    pos = Bytes.putBytes(kvBytes, pos, key, 0, key.length);
    Bytes.putBytes(kvBytes, pos, value, 0, value.length);
    return new KeyValue(kvBytes);
  }
}
