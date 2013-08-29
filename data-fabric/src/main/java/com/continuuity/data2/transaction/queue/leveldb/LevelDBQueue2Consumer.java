/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.AbstractQueue2Consumer;
import com.continuuity.data2.transaction.queue.QueueEvictor;
import com.continuuity.data2.transaction.queue.QueueScanner;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Queue consumer for levelDB
 */
public final class LevelDBQueue2Consumer extends AbstractQueue2Consumer {

  // used for undoing state. The value does not matter, but the OcTable interface was written to expect some value...
  private static final byte[] DUMMY_STATE_CONTENT = { };
  private static final long[] NO_EXCLUDES = { };
  private static final Transaction ALL_LATEST_TRANSACTION =
    new Transaction(KeyValue.LATEST_TIMESTAMP, KeyValue.LATEST_TIMESTAMP, NO_EXCLUDES);

  private final LevelDBOcTableCore core;
  private final Object lock;

  LevelDBQueue2Consumer(LevelDBOcTableCore tableCore, Object queueLock, ConsumerConfig consumerConfig,
                        QueueName queueName, QueueEvictor queueEvictor) {
    super(consumerConfig, queueName, queueEvictor);
    core = tableCore;
    lock = queueLock;
  }

  private final NavigableMap<byte[], NavigableMap<byte[], byte[]>>
    rowMapForClaim = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  private final NavigableMap<byte[], byte[]>
    colMapForClaim = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

  @Override
  protected boolean claimEntry(byte[] rowKey, byte[] stateContent) throws IOException {
    synchronized (this.lock) {
      Map<byte[], byte[]> row =
        core.getRow(rowKey, new byte[][] { stateColumnName }, null, null, -1, ALL_LATEST_TRANSACTION);
      if (row.get(stateColumnName) != null) {
        return false;
      }
      rowMapForClaim.clear();
      colMapForClaim.clear();
      colMapForClaim.put(stateColumnName, stateContent);
      rowMapForClaim.put(rowKey, colMapForClaim);

      core.persist(rowMapForClaim, KeyValue.LATEST_TIMESTAMP);
      return true;
    }
  }

  @Override
  protected void updateState(Set<byte[]> rowKeys, byte[] stateColumnName, byte[] stateContent) throws IOException {
    if (rowKeys.isEmpty()) {
      return;
    }
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] rowKey : rowKeys) {
      NavigableMap<byte[], byte[]> row = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      row.put(stateColumnName, stateContent);
      changes.put(rowKey, row);
    }
    core.persist(changes, KeyValue.LATEST_TIMESTAMP);
  }

  @Override
  protected void undoState(Set<byte[]> rowKeys, byte[] stateColumnName) throws IOException, InterruptedException {
    if (rowKeys.isEmpty()) {
      return;
    }
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] rowKey : rowKeys) {
      NavigableMap<byte[], byte[]> row = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      row.put(stateColumnName, DUMMY_STATE_CONTENT);
      changes.put(rowKey, row);
    }
    core.undo(changes, KeyValue.LATEST_TIMESTAMP);
  }

  @Override
  protected QueueScanner getScanner(byte[] startRow, byte[] stopRow, int numRows) throws IOException {
    final Scanner scanner = core.scan(startRow, stopRow, ALL_LATEST_TRANSACTION);
    return new QueueScanner() {
      @Override
      public ImmutablePair<byte[], Map<byte[], byte[]>> next() throws IOException {
        return scanner.next();
      }

      @Override
      public void close() throws IOException {
        scanner.close();
      }
    };
  }
}
