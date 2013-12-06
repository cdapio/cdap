/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.leveldb.KeyValue;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.AbstractQueue2Consumer;
import com.continuuity.data2.transaction.queue.QueueEvictor;
import com.continuuity.data2.transaction.queue.QueueScanner;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Queue consumer for levelDB.
 */
public final class LevelDBQueue2Consumer extends AbstractQueue2Consumer {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBQueue2Consumer.class);

  // How many commits to trigger eviction.
  private static final int EVICTION_LIMIT = 1000;

  private static final long EVICTION_TIMEOUT_SECONDS = 10;

  // used for undoing state. The value does not matter, but the OcTable interface was written to expect some value...
  private static final byte[] DUMMY_STATE_CONTENT = { };

  private final QueueEvictor queueEvictor;
  private final LevelDBOcTableCore core;
  private final Object lock;
  private final NavigableMap<byte[], NavigableMap<byte[], byte[]>>
    rowMapForClaim = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  private final NavigableMap<byte[], byte[]>
    colMapForClaim = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

  LevelDBQueue2Consumer(LevelDBOcTableCore tableCore, Object queueLock, ConsumerConfig consumerConfig,
                        QueueName queueName, QueueEvictor queueEvictor) {
    super(consumerConfig, queueName);
    this.queueEvictor = queueEvictor;
    core = tableCore;
    lock = queueLock;
  }

  @Override
  public void postTxCommit() {
    super.postTxCommit();
    if (commitCount > EVICTION_LIMIT && transaction != null) {
      // Fire and forget eviction.
      queueEvictor.evict(transaction);
      commitCount = 0;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (transaction != null) {
        // Use whatever last transaction for eviction.
        // Has to block until eviction is completed
        Uninterruptibles.getUninterruptibly(queueEvictor.evict(transaction),
                                            EVICTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    } catch (ExecutionException e) {
      LOG.warn("Failed to perform queue eviction.", e.getCause());
    } catch (TimeoutException e) {
      LOG.warn("Timeout when performing queue eviction.", e);
    }
  }

  @Override
  protected boolean claimEntry(byte[] rowKey, byte[] stateContent) throws IOException {
    synchronized (this.lock) {
      Map<byte[], byte[]> row =
        core.getRow(rowKey, new byte[][] { stateColumnName }, null, null, -1, Transaction.ALL_VISIBLE_LATEST);
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
    final Scanner scanner = core.scan(startRow, stopRow, null, null, Transaction.ALL_VISIBLE_LATEST);
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
