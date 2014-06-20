package com.continuuity.data2.dataset2.lib.table.leveldb;

import com.continuuity.api.dataset.table.ConflictDetection;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.dataset2.lib.table.BackedByVersionedStoreOrderedTable;
import com.continuuity.data2.transaction.Transaction;

import java.io.IOException;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * A table client based on LevelDB.
 */
public class LevelDBOrderedTable extends BackedByVersionedStoreOrderedTable {

  private final LevelDBOrderedTableCore core;
  private Transaction tx;
  private long persistedVersion;

  public LevelDBOrderedTable(String tableName, LevelDBOcTableService service, ConflictDetection level)
    throws IOException {
    super(tableName, level);
    this.core = new LevelDBOrderedTableCore(tableName, service);
  }

  // TODO this is the same for all OrderedTableClient implementations -> promote to base class
  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes) throws Exception {
    persistedVersion = tx == null ? System.currentTimeMillis() : tx.getWritePointer();
    core.persist(changes, persistedVersion);
  }

  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], byte[]>> persisted) throws Exception {
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
