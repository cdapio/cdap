/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.NavigableMap;

/**
 * unit-test
 * @param <T> table type
 */
public abstract class BufferingTableTest<T extends BufferingTable>
  extends TableConcurrentTest<T> {

  @Test
  public void testRollingBackAfterExceptionDuringPersist() throws Exception {
    DatasetAdmin admin = getTableAdmin(MY_TABLE);
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      BufferingTable myTable1 =
        new BufferingTableWithPersistingFailure(getTable(MY_TABLE));
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
      Table myTable2 = getTable(MY_TABLE);
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
    String testScanWithBuffering = DS_NAMESPACE.namespace(NAMESPACE_ID, "testScanWithBuffering");
    DatasetAdmin admin = getTableAdmin(testScanWithBuffering);
    admin.create();
    try {
      //
      Transaction tx1 = txClient.startShort();
      Table table1 = getTable(testScanWithBuffering);
      ((TransactionAware) table1).startTx(tx1);

      table1.put(Bytes.toBytes("1_01"), a(C1), a(V1));
      table1.put(Bytes.toBytes("1_02"), a(C1), a(V1));
      table1.put(Bytes.toBytes("1_03"), a(C1), a(V1));

      // written values should not yet be persisted
      verify(new byte[0][],
             new byte[0][][],
             ((BufferingTable) table1).scanPersisted(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

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
             ((BufferingTable) table1).scanPersisted(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

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

  @Test
  public void testChangingParamsAndReturnValues() throws Exception {
    // The test verifies that one can re-use byte arrays passed as parameters to write methods of a table without
    // affecting the stored data.
    // Also, one can re-use (modify) returned data from the table without affecting the stored data.
    String myTable = DS_NAMESPACE.namespace(NAMESPACE_ID, "myTable");
    DatasetAdmin admin = getTableAdmin(myTable);
    admin.create();
    try {
      // writing some data: we'll need it to test delete later
      Transaction tx = txClient.startShort();
      BufferingTable table = getTable(myTable);
      table.startTx(tx);

      table.put(new byte[] {0}, new byte[] {9}, new byte[] {8});

      table.commitTx();
      txClient.commit(tx);

      // start new for in-mem buffer behavior testing
      tx = txClient.startShort();
      table.startTx(tx);

      // write some data but not commit
      byte[] rowParam = new byte[] {1};
      byte[] colParam = new byte[] {2};
      byte[] valParam = Bytes.toBytes(3L);
      table.put(rowParam, colParam, valParam);

      verify123(table);

      // change passed earlier byte arrays in place, this should not affect stored previously values
      rowParam[0]++;
      colParam[0]++;
      valParam[0]++;

      verify123(table);

      // try get row and change returned values in place, which should not affect the data stored
      Row getRow = table.get(new byte[] {1});
      Map<byte[], byte[]> getRowResult = getRow.getColumns();
      Assert.assertEquals(1, getRowResult.size());
      byte[] colFromGetRow = getRowResult.keySet().iterator().next();
      byte[] valFromGetRow = getRowResult.get(colFromGetRow);
      getRowResult.remove(new byte[] {2});

      Assert.assertArrayEquals(new byte[] {2}, colFromGetRow);
      Assert.assertArrayEquals(Bytes.toBytes(3L), valFromGetRow);

      colFromGetRow[0]++;
      valFromGetRow[0]++;

      verify123(table);

      // try get set of columns in a row and change returned values in place, which should not affect the data stored
      Row getColumnSetRow = table.get(new byte[] {1});
      Map<byte[], byte[]> getColumnSetResult = getColumnSetRow.getColumns();
      Assert.assertEquals(1, getColumnSetResult.size());
      byte[] colFromGetColumnSet = getColumnSetResult.keySet().iterator().next();
      byte[] valFromGetColumnSet = getColumnSetResult.values().iterator().next();
      getColumnSetResult.remove(new byte[] {2});

      Assert.assertArrayEquals(new byte[] {2}, colFromGetColumnSet);
      Assert.assertArrayEquals(Bytes.toBytes(3L), valFromGetColumnSet);

      colFromGetColumnSet[0]++;
      valFromGetColumnSet[0]++;

      verify123(table);

      // try get column and change returned value in place, which should not affect the data stored
      byte[] valFromGetColumn = table.get(new byte[] {1}, new byte[] {2});
      Assert.assertArrayEquals(Bytes.toBytes(3L), valFromGetColumn);

      valFromGetColumn[0]++;

      verify123(table);

      // try scan and change returned value in place, which should not affect the data stored
      Scanner scan = table.scan(new byte[] {1}, null);
      Row next = scan.next();
      Assert.assertNotNull(next);
      byte[] rowFromScan = next.getRow();
      Assert.assertArrayEquals(new byte[] {1}, rowFromScan);
      Map<byte[], byte[]> cols = next.getColumns();
      Assert.assertEquals(1, cols.size());
      byte[] colFromScan = cols.keySet().iterator().next();
      Assert.assertArrayEquals(new byte[] {2}, colFromScan);
      byte[] valFromScan = next.get(new byte[] {2});
      Assert.assertNotNull(valFromScan);
      Assert.assertArrayEquals(Bytes.toBytes(3L), valFromScan);
      Assert.assertNull(scan.next());
      cols.remove(new byte[] {2});

      rowFromScan[0]++;
      colFromScan[0]++;
      valFromScan[0]++;

      verify123(table);

      // try delete and change params in place: this should not affect stored data
      rowParam = new byte[] {1};
      colParam = new byte[] {2};
      table.delete(rowParam, colParam);

      Assert.assertNull(table.get(new byte[] {1}, new byte[] {2}));
      Assert.assertArrayEquals(new byte[] {8}, table.get(new byte[] {0}, new byte[] {9}));

      rowParam[0] = 0;
      colParam[0] = 9;

      Assert.assertNull(table.get(new byte[] {1}, new byte[] {2}));
      Assert.assertArrayEquals(new byte[] {8}, table.get(new byte[] {0}, new byte[] {9}));

      // try increment column and change params in place: this should not affect stored data
      byte[] rowIncParam = new byte[] {1};
      byte[] colIncParam = new byte[] {2};
      table.increment(rowIncParam, colIncParam, 3);

      verify123(table);

      rowIncParam[0]++;
      colIncParam[0]++;

      verify123(table);

      // try increment set of columns and change params in place, try also to change values in returned map: this all
      // should not affect stored data.

      rowIncParam = new byte[] {1};
      colIncParam = new byte[] {2};
      table.increment(rowIncParam, colIncParam, -1);
      table.increment(rowIncParam, new byte[][] {colIncParam}, new long[] {1});

      verify123(table);

      rowIncParam[0]++;
      colIncParam[0]++;

      verify123(table);

      // try increment and change returned values: should not affect the stored data
      rowIncParam = new byte[] {1};
      colIncParam = new byte[] {2};
      table.increment(rowIncParam, colIncParam, -1);
      Row countersRow = table.incrementAndGet(rowIncParam, new byte[][] {colIncParam}, new long[] {1});
      Map<byte[], byte[]> counters = countersRow.getColumns();

      Assert.assertEquals(1, counters.size());
      byte[] colFromInc = counters.keySet().iterator().next();
      Assert.assertArrayEquals(new byte[] {2}, colFromInc);
      Assert.assertEquals(3, (long) Bytes.toLong(counters.get(colFromInc)));
      counters.remove(new byte[] {2});
      colFromInc[0]++;

      verify123(table);

      // try increment write and change params in place: this should not affect stored data
      rowIncParam = new byte[] {1};
      colIncParam = new byte[] {2};
      table.increment(rowIncParam, colIncParam, -1);
      table.increment(rowIncParam, new byte[][] {colIncParam}, new long[] {1});

      verify123(table);

      rowIncParam[0]++;
      colIncParam[0]++;

      verify123(table);

      // try compareAndSwap and change params in place: this should not affect stored data
      byte[] rowSwapParam = new byte[] {1};
      byte[] colSwapParam = new byte[] {2};
      byte[] valSwapParam = Bytes.toBytes(3L);
      table.compareAndSwap(rowSwapParam, colSwapParam, Bytes.toBytes(3L), Bytes.toBytes(4L));
      table.compareAndSwap(rowSwapParam, colSwapParam, Bytes.toBytes(4L), valSwapParam);

      verify123(table);

      rowSwapParam[0]++;
      colSwapParam[0]++;
      valSwapParam[0]++;

      verify123(table);

      // We don't care to persist changes and commit tx here: we tested what we wanted
    } finally {
      admin.drop();
    }
  }

  private void verify123(BufferingTable table) throws Exception {
    byte[] row = new byte[] {1};
    byte[] col = new byte[] {2};
    byte[] val = Bytes.toBytes((long) 3);
    verify(table, row, col, val);
    row[0]++;
    col[0]++;
    val[0]++;
    Assert.assertNull(table.get(row, col));
    Assert.assertTrue(table.get(row).isEmpty());

    Assert.assertArrayEquals(new byte[] {8}, table.get(new byte[] {0}, new byte[] {9}));
  }

  private void verify(BufferingTable table, byte[] row, byte[] col, byte[] val) throws Exception {
    // get column
    Assert.assertArrayEquals(val, table.get(row, col));

    // get set of columns
    Row getColSetRow = table.get(row, new byte[][] {col});
    Map<byte[], byte[]> getColSetResult = getColSetRow.getColumns();
    Assert.assertEquals(1, getColSetResult.size());
    Assert.assertArrayEquals(val, getColSetResult.get(col));

    // get row
    Row getRow = table.get(row);
    Map<byte[], byte[]> getRowResult = getRow.getColumns();
    Assert.assertEquals(1, getRowResult.size());
    Assert.assertArrayEquals(val, getRowResult.get(col));

    // scan
    Scanner scan = table.scan(row, null);
    Row next = scan.next();
    Assert.assertNotNull(next);
    Assert.assertArrayEquals(row, next.getRow());
    Assert.assertArrayEquals(val, next.get(col));
    Assert.assertNull(scan.next());
  }

  // This class looks weird, this is what we have to do to override persist method to make it throw exception in the
  // middle. NOTE: We want to test how every implementation of BufferingTable handles undoing changes in this
  // case, otherwise we would just test the method of BufferingTable directly.

  public static class BufferingTableWithPersistingFailure extends BufferingTable {
    private BufferingTable delegate;
    private final byte[] nameAsTxChangePrefix;

    public BufferingTableWithPersistingFailure(BufferingTable delegate) {
      super(delegate.getTableName());
      this.delegate = delegate;
      String delegateTableName = delegate.getTableName();
      this.nameAsTxChangePrefix = Bytes.add(new byte[]{(byte) delegateTableName.length()},
                                            Bytes.toBytes(delegateTableName));
    }

    // override persist to simulate failure in the middle

    @Override
    public byte[] getNameAsTxChangePrefix() {
      return nameAsTxChangePrefix;
    }

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
