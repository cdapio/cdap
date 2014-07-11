package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.data2.dataset.lib.table.BackedByVersionedStoreOcTableClient;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.IncrementValue;
import com.continuuity.data2.dataset.lib.table.PutValue;
import com.continuuity.data2.dataset.lib.table.Update;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * A table client based on LevelDB.
 */
public class LevelDBOcTableClient extends BackedByVersionedStoreOcTableClient {

  private final LevelDBOcTableCore core;
  private Transaction tx;
  private long persistedVersion;

  public LevelDBOcTableClient(String tableName, LevelDBOcTableService service) throws IOException {
    this(tableName, ConflictDetection.ROW, service);
  }

  public LevelDBOcTableClient(String tableName, ConflictDetection level, LevelDBOcTableService service)
    throws IOException {
    super(tableName, level);
    this.core = new LevelDBOcTableCore(tableName, service);
  }

  // TODO this is the same for all OcTableClient implementations -> promote to base class
  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public void incrementWrite(byte[] row, byte[][] columns, long[] amounts) throws Exception {
    // for local operation with leveldb, we don't worry about the cost of reads
    increment(row, columns, amounts);
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
    return core.getRow(row, columns, null, null, columns == null ? Integer.MAX_VALUE : columns.length, tx);
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
