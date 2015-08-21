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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.KeyValue;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.queue.AbstractQueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.tephra.Transaction;
import com.google.common.collect.Maps;

import java.util.NavigableMap;

/**
 * A queue producer for levelDB.
 */
public final class LevelDBQueueProducer extends AbstractQueueProducer {

  private final LevelDBTableCore core;
  private final byte[] queueRowPrefix;
  private final NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes;

  public LevelDBQueueProducer(LevelDBTableCore tableCore, QueueName queueName, QueueMetrics queueMetrics) {
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
