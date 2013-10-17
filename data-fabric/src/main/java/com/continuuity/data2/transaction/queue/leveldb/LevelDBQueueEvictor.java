package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.common.queue.QueueName;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.QueueEvictor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * An evictor for the levelDB based queues.
 */
public class LevelDBQueueEvictor implements QueueEvictor {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBQueueEvictor.class);

  private final LevelDBOcTableCore core;
  private final byte[] queueRowPrefix;
  private final Executor executor;
  private final int numGroups;
  private final QueueName name;

  public LevelDBQueueEvictor(LevelDBOcTableCore core, QueueName queueName, int numGroups, Executor executor) {
    this.core = core;
    this.executor = executor;
    this.queueRowPrefix = QueueEntryRow.getQueueRowPrefix(queueName);
    this.numGroups = numGroups;
    this.name = queueName;
  }

  @Override
  public ListenableFuture<Integer> evict(final Transaction transaction) {
    final SettableFuture<Integer> result = SettableFuture.create();
    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          result.set(doEvict(transaction));
        } catch (Throwable t) {
          result.setException(t);
        }
      }
    });
    return result;
  }

  private synchronized int doEvict(Transaction transaction) throws IOException {
    final byte[] stopRow = QueueEntryRow.getStopRowForTransaction(queueRowPrefix, transaction);
    ImmutablePair<byte[], Map<byte[], byte[]>> row;
    List<byte[]> rowsToDelete = Lists.newArrayList();
    // the scan must be non-transactional in order to see the state columns (which have latest timestamp)
    Scanner scanner = core.scan(queueRowPrefix, stopRow, null, null, Transaction.ALL_VISIBLE_LATEST);
    try {
      while ((row = scanner.next()) != null) {
        int processed = 0;
        for (Map.Entry<byte[], byte[]> entry : row.getSecond().entrySet()) {
          // is it a state column for a consumer instance?
          if (!QueueEntryRow.isStateColumn(entry.getKey())) {
            continue;
          }
          // is the write pointer of this state committed w.r.t. the current transaction, and is it processed?
          if (QueueEntryRow.isCommittedProcessed(entry.getValue(), transaction)) {
            ++processed;
          }
        }
        if (processed >= numGroups) {
          rowsToDelete.add(row.getFirst());
        }
      }
    } finally {
      scanner.close();
    }
    if (!rowsToDelete.isEmpty()) {
      core.deleteRows(rowsToDelete);
      LOG.trace("Evicted {} entries from queue {}", rowsToDelete.size(), name);
    } else {
      LOG.trace("Nothing to evict from queue {}", name);
    }
    return rowsToDelete.size();
  }
}
