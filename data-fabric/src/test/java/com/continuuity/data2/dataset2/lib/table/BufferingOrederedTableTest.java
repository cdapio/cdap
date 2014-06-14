package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.NavigableMap;

/**
 * unit-test
 * @param <T> table type
 */
public abstract class BufferingOrederedTableTest<T extends BufferingOrderedTable>
  extends OrderedTableConcurrentTest<T> {

  @Test
  public void testRollingBackAfterExceptionDuringPersist() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      BufferingOrderedTable myTable1 = new BufferingOrderedTableWithPersistingFailure(getTable("myTable"));
      myTable1.startTx(tx1);
      // write some data but not commit
      myTable1.put(R1, a(C1), a(V1));
      myTable1.put(R2, a(C2), a(V2));
      // verify can see changes inside tx
      verify(a(C1, V1), myTable1.get(R1, a(C1)));
      verify(a(C2, V2), myTable1.get(R2, a(C2)));

      // persisting changes
      try {
        // should simulate exception
        myTable1.commitTx();
        Assert.assertFalse(true);
      } catch (Throwable th) {
        // Expected simulated exception
      }

      // let's pretend that after persisting changes we still got conflict when finalizing tx, so
      // rolling back changes
      Assert.assertTrue(myTable1.rollbackTx());

      // making tx visible
      txClient.abort(tx1);

      // start new tx
      Transaction tx2 = txClient.startShort();
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify don't see rolled back changes
      verify(a(), myTable2.get(R1, a(C1)));
      verify(a(), myTable2.get(R2, a(C2)));

    } finally {
      admin.drop();
    }
  }

  // This class looks weird, this is what we have to do to override persist method to make it throw exception in the
  // middle. NOTE: We want to test how every implementation of BufferingOcTableClient handles undoing changes in this
  // case, otherwise we would just test the method of BufferingOcTableClient directly.

  private static class BufferingOrderedTableWithPersistingFailure extends BufferingOrderedTable {
    private BufferingOrderedTable delegate;

    private BufferingOrderedTableWithPersistingFailure(BufferingOrderedTable delegate) {
      super(delegate.getName(), delegate.getConflictLevel());
      this.delegate = delegate;
    }

    // override persist to simulate failure in the middle

    @Override
    protected void persist(NavigableMap<byte[], NavigableMap<byte[], byte[]>> buff) throws Exception {
      // persists only first change and throws exception
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> toPersist = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      if (buff.size() > 0) {
        toPersist.put(buff.firstEntry().getKey(), buff.firstEntry().getValue());
      }
      delegate.persist(toPersist);
      throw new RuntimeException("Simulating failure in the middle of persist");
    }

    // implementing abstract methods

    @Override
    protected void undo(NavigableMap<byte[], NavigableMap<byte[], byte[]>> persisted) throws Exception {
      delegate.undo(persisted);
    }

    @Override
    protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[][] columns) throws Exception {
      return delegate.getPersisted(row, columns);
    }

    @Override
    protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
      throws Exception {
      return delegate.getPersisted(row, startColumn, stopColumn, limit);
    }

    @Override
    protected Scanner scanPersisted(byte[] startRow, byte[] stopRow) throws Exception {
      return delegate.scanPersisted(startRow, stopRow);
    }

    // propagating tx to delegate

    @Override
    public void startTx(Transaction tx) {
      super.startTx(tx);
      delegate.startTx(tx);
    }
  }
}
