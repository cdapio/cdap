package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClient;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import org.junit.Assert;
import org.junit.Test;

/**
 * unit-test
 * @param <T> table type
 */
public abstract class BufferingOrederedTableTest<T extends BufferingOcTableClient>
  extends OrderedTableConcurrentTest<T> {

  @Test
  public void testRollingBackAfterExceptionDuringPersist() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      BufferingOcTableClient myTable1 =
        new BufferingOcTableClientTest.BufferingOcTableWithPersistingFailure(getTable("myTable"));
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
}
