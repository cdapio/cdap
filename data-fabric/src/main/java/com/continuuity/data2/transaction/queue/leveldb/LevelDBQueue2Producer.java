/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.dataset.lib.table.leveldb.KeyValue;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.AbstractQueue2Producer;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.google.common.collect.Maps;

import java.util.NavigableMap;

/**
 * A queue producer for levelDB.
 */
public final class LevelDBQueue2Producer extends AbstractQueue2Producer {

  private final LevelDBOcTableCore core;
  private final byte[] queueRowPrefix;
  private final NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes;

  public LevelDBQueue2Producer(LevelDBOcTableCore tableCore, QueueName queueName, QueueMetrics queueMetrics) {
    super(queueMetrics, queueName);
    core = tableCore;
    changes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    changes.clear();
  }

  @Override
  protected int persist(Iterable<QueueEntry> entries, Transaction transaction) throws Exception {
    long writePointer = transaction.getWritePointer();
    byte[] rowKeyPrefix = Bytes.add(queueRowPrefix, Bytes.toBytes(writePointer));
    int count = 0;
    int bytes = 0;

    for (QueueEntry entry : entries) {
      // Row key = queue_name + writePointer + counter
      byte[] rowKey = Bytes.add(rowKeyPrefix, Bytes.toBytes(count++));
      NavigableMap<byte[], byte[]> row = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      row.put(QueueEntryRow.DATA_COLUMN, entry.getData());
      row.put(QueueEntryRow.META_COLUMN, QueueEntry.serializeHashKeys(entry.getHashKeys()));
      changes.put(rowKey, row);
      bytes += entry.getData().length;
    }
    // TODO introduce a constant in the OcTableCore for the latest timestamp
    core.persist(changes, KeyValue.LATEST_TIMESTAMP);

    return bytes;
  }

  @Override
  protected void doRollback() throws Exception {
    core.undo(changes, KeyValue.LATEST_TIMESTAMP);
  }
}
