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

package co.cask.cdap.data2.dataset2.lib.table.ordered;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.nio.Buffer;
import java.util.NavigableMap;

/**
 * unit-test
 * @param <T> table type
 */
public abstract class BufferingOrderedTableTest<T extends BufferingOrderedTable>
  extends OrderedTableConcurrentTest<T> {

  @Test
  public void testRollingBackAfterExceptionDuringPersist() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      BufferingOrderedTable myTable1 =
        new BufferingOrderedTableWithPersistingFailure(getTable("myTable"));
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

  /**
   * Tests that writes being buffered in memory by the client are still visible during scans.
   */
  @Test
  public void testScanWithBuffering() throws Exception {
    DatasetAdmin admin = getTableAdmin("testScanWithBuffering");
    admin.create();
    try {
      //
      Transaction tx1 = txClient.startShort();
      OrderedTable table1 = getTable("testScanWithBuffering");
      ((TransactionAware) table1).startTx(tx1);

      table1.put(Bytes.toBytes("1_01"), a(C1), a(V1));
      table1.put(Bytes.toBytes("1_02"), a(C1), a(V1));
      table1.put(Bytes.toBytes("1_03"), a(C1), a(V1));

      // written values should not yet be persisted
      verify(new byte[0][],
             new byte[0][][],
             ((BufferingOrderedTable) table1).scanPersisted(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

      // buffered values should be visible in a scan
      verify(a(Bytes.toBytes("1_01"), Bytes.toBytes("1_02"), Bytes.toBytes("1_03")),
             aa(a(C1, V1),
                a(C1, V1),
                a(C1, V1)),
             table1.scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) table1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      Transaction tx2 = txClient.startShort();
      ((TransactionAware) table1).startTx(tx2);

      // written values should be visible after commit
      verify(a(Bytes.toBytes("1_01"), Bytes.toBytes("1_02"), Bytes.toBytes("1_03")),
             aa(a(C1, V1),
                a(C1, V1),
                a(C1, V1)),
             table1.scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

      txClient.commit(tx2);

      Transaction tx3 = txClient.startShort();
      ((TransactionAware) table1).startTx(tx3);

      // test merging of buffered writes on existing rows
      table1.put(Bytes.toBytes("1_01"), a(C2), a(V2));
      table1.put(Bytes.toBytes("1_02"), a(C1), a(V2));
      table1.put(Bytes.toBytes("1_02a"), a(C1), a(V1));
      table1.put(Bytes.toBytes("1_02b"), a(C1), a(V1));
      table1.put(Bytes.toBytes("1_04"), a(C2), a(V2));

      // persisted values should be the same
      verify(a(Bytes.toBytes("1_01"), Bytes.toBytes("1_02"), Bytes.toBytes("1_03")),
             aa(a(C1, V1),
                a(C1, V1),
                a(C1, V1)),
             ((BufferingOrderedTable) table1).scanPersisted(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

      // all values should be visible in buffered scan
      verify(a(Bytes.toBytes("1_01"), Bytes.toBytes("1_02"), Bytes.toBytes("1_02a"), Bytes.toBytes("1_02b"),
               Bytes.toBytes("1_03"), Bytes.toBytes("1_04")),
             aa(a(C1, V1, C2, V2), // 1_01
                a(C1, V2),         // 1_02
                a(C1, V1),         // 1_02a
                a(C1, V1),         // 1_02b
                a(C1, V1),         // 1_03
                a(C2, V2)),        // 1_04
             table1.scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) table1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table1).commitTx());
      txClient.commit(tx3);

      Transaction tx4 = txClient.startShort();
      ((TransactionAware) table1).startTx(tx4);

      // all values should be visible after commit
      verify(a(Bytes.toBytes("1_01"), Bytes.toBytes("1_02"), Bytes.toBytes("1_02a"), Bytes.toBytes("1_02b"),
               Bytes.toBytes("1_03"), Bytes.toBytes("1_04")),
             aa(a(C1, V1, C2, V2), // 1_01
                a(C1, V2),         // 1_02
                a(C1, V1),         // 1_02a
                a(C1, V1),         // 1_02b
                a(C1, V1),         // 1_03
                a(C2, V2)),        // 1_04
             table1.scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

      txClient.commit(tx4);
    } finally {
      admin.drop();
    }
  }

  // This class looks weird, this is what we have to do to override persist method to make it throw exception in the
  // middle. NOTE: We want to test how every implementation of BufferingOrderedTable handles undoing changes in this
  // case, otherwise we would just test the method of BufferingOrderedTable directly.

  public static class BufferingOrderedTableWithPersistingFailure extends BufferingOrderedTable {
    private BufferingOrderedTable delegate;

    public BufferingOrderedTableWithPersistingFailure(BufferingOrderedTable delegate) {
      super(delegate.getTableName());
      this.delegate = delegate;
    }

    // override persist to simulate failure in the middle

    @Override
    protected void persist(NavigableMap<byte[], NavigableMap<byte[], Update>> buff) throws Exception {
      // persists only first change and throws exception
      NavigableMap<byte[], NavigableMap<byte[], Update>> toPersist = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      if (buff.size() > 0) {
        toPersist.put(buff.firstEntry().getKey(), buff.firstEntry().getValue());
      }
      delegate.persist(toPersist);
      throw new RuntimeException("Simulating failure in the middle of persist");
    }

    // implementing abstract methods

    @Override
    protected void undo(NavigableMap<byte[], NavigableMap<byte[], Update>> persisted) throws Exception {
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
