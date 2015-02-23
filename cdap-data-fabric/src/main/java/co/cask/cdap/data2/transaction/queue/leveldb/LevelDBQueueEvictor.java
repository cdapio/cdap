/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.leveldb;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueEvictor;
import co.cask.tephra.Transaction;
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

  private final LevelDBTableCore core;
  private final byte[] queueRowPrefix;
  private final Executor executor;
  private final int numGroups;
  private final QueueName name;

  public LevelDBQueueEvictor(LevelDBTableCore core, QueueName queueName, int numGroups, Executor executor) {
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
    Row row;
    List<byte[]> rowsToDelete = Lists.newArrayList();
    // the scan must be non-transactional in order to see the state columns (which have latest timestamp)
    Scanner scanner = core.scan(queueRowPrefix, stopRow, null, null, Transaction.ALL_VISIBLE_LATEST);
    try {
      while ((row = scanner.next()) != null) {
        int processed = 0;
        for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
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
          rowsToDelete.add(row.getRow());
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
