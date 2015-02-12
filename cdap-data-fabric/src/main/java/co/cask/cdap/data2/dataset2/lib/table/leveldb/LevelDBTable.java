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

package co.cask.cdap.data2.dataset2.lib.table.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTable;
import co.cask.cdap.data2.dataset2.lib.table.IncrementValue;
import co.cask.cdap.data2.dataset2.lib.table.PutValue;
import co.cask.cdap.data2.dataset2.lib.table.Update;
import co.cask.tephra.Transaction;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * A table client based on LevelDB.
 */
public class LevelDBTable extends BufferingTable {

  private final LevelDBTableCore core;
  private Transaction tx;
  private long persistedVersion;

  public LevelDBTable(String tableName, ConflictDetection level, LevelDBTableService service)
    throws IOException {
    super(tableName, level);
    this.core = new LevelDBTableCore(tableName, service);
  }

  // TODO this is the same for all OcTableClient implementations -> promote to base class
  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    // for local operation with leveldb, we don't worry about the cost of reads
    incrementAndGet(row, columns, amounts);
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], Update>> changes) throws Exception {
    persistedVersion = tx == null ? System.currentTimeMillis() : tx.getWritePointer();

    NavigableMap<byte[], NavigableMap<byte[], byte[]>> puts = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<byte[], Long>> increments = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> rowEntry : changes.entrySet()) {
      for (Map.Entry<byte[], Update> colEntry : rowEntry.getValue().entrySet()) {
        Update val = colEntry.getValue();
        if (val instanceof IncrementValue) {
          NavigableMap<byte[], Long> incrCols = increments.get(rowEntry.getKey());
          if (incrCols == null) {
            incrCols = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            increments.put(rowEntry.getKey(), incrCols);
          }
          incrCols.put(colEntry.getKey(), ((IncrementValue) val).getValue());
        } else if (val instanceof PutValue) {
          NavigableMap<byte[], byte[]> putCols = puts.get(rowEntry.getKey());
          if (putCols == null) {
            putCols = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            puts.put(rowEntry.getKey(), putCols);
          }
          putCols.put(colEntry.getKey(), ((PutValue) val).getValue());
        }
      }
    }
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> incEntry : increments.entrySet()) {
      core.increment(incEntry.getKey(), incEntry.getValue());
    }
    core.persist(puts, persistedVersion);
  }

  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], Update>> persisted) throws Exception {
    core.undo(persisted, persistedVersion);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, @Nullable byte[][] columns) throws Exception {
    return core.getRow(row, columns, null, null, -1, tx);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception {
    return core.getRow(row, null, startColumn, stopColumn, limit, tx);
  }

  @Override
  protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) throws Exception {
    return core.scan(startRow, stopRow, null, null, tx);
  }
}
