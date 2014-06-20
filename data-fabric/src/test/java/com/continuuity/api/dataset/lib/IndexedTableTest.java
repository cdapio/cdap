package com.continuuity.api.dataset.lib;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.table.Delete;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.data2.dataset2.AbstractDatasetTest;
import com.continuuity.data2.dataset2.TableTest;
import com.continuuity.data2.dataset2.lib.table.CoreDatasetsModule;
import com.continuuity.data2.transaction.TransactionExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for Index table.
 */
public class IndexedTableTest extends AbstractDatasetTest {

  private IndexedTable table;

  static byte[] idxCol = { 'i', 'd', 'x' };
  static byte[] valCol = { 'v', 'a', 'l' };
  static byte[] keyA = { 'a' };
  static byte[] keyAA = { 'a', 'a' };
  static byte[] keyB = { 'b' };
  static byte[] keyC = { 'c' };
  static byte[] valA = { 'a' };
  static byte[] valAA = { 'a', 'a' };
  static byte[] valAB = { 'a', 'b' };
  static byte[] valB = { 'b' };
  static byte[] valBB = { 'b', 'b' };
  static byte[] valC = { 'c' };
  static byte[] idx1 = { '1' };
  static byte[] idx2 = { '2' };
  static byte[] idx3 = { '3' };

  static String idxColString = Bytes.toString(idxCol);
  static byte[][] colIdxVal = { idxCol, valCol };

  @Before
  public void setUp() throws Exception {
    super.setUp();

    addModule("core", new CoreDatasetsModule());
    createInstance("indexedTable", "tab", DatasetProperties.builder()
      .add("columnToIndex", idxColString)
      .build());
    table = getInstance("tab");
  }

  @After
  public void tearDown() throws Exception {
    deleteInstance("tab");
    deleteModule("core");
    super.tearDown();
  }

  @Test
  public void testIndexedOperations() throws Exception {
    TransactionExecutor txnl = newTransactionExecutor(table);

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // add a value c with idx = 1, and b with idx = 2
        table.put(new Put(keyC).add(idxCol, idx1).add(valCol, valC));
        table.put(new Put(keyB).add(idxCol, idx2).add(valCol, valB));
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // read by key c
        Row row = table.get(new Get(keyC, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
        // read by key b
        row = table.get(new Get(keyB, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valB});
        // read by idx 1 -> c
        row = table.readBy(new Get(idx1, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
        // read by idx 2 -> b
        row = table.readBy(new Get(idx2, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][] { idx2, valB });
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // add a value a with idx = 1
        table.put(new Put(keyA).add(idxCol, idx1).add(valCol, valA));
      }
    });

    // read by idx 1 -> a
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = table.readBy(new Get(idx1, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valA});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // delete value a
        table.delete(new Delete(keyA, colIdxVal));
      }
    });

    // read by idx 1 -> c
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = table.readBy(new Get(idx1, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // add a value aa with idx 2
        table.put(new Put(keyAA).add(idxCol, idx2).add(valCol, valAA));
      }
    });

    // read by idx 2 -> aa
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = table.readBy(new Get(idx2, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valAA});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap value for aa to ab
        Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valAA, valAB));
      }
    });

    // read by idx 2 -> ab
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = table.readBy(new Get(idx2, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valAB});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap value for aa to bb
        Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valAB, valBB));
      }
    });

    // read by idx 2 -> bb (value of key aa)
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = table.readBy(new Get(idx2, colIdxVal));
        TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valBB});
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap value for aa to null
        Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valBB, null));
      }
    });

    // read by idx 2 -> null (value of b)
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Row row = table.readBy(new Get(idx2, colIdxVal));
        TableTest.verifyColumn(row, idxCol, idx2);
      }
    });

    // start a new transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap idx for c to 3
        Assert.assertTrue(table.compareAndSwap(keyC, idxCol, idx1, idx3));
      }
    });

    // read by idx 1 -> null (no row has that any more)
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertNull(table.readBy(new Get(idx1, colIdxVal)).get(idx2));
        // read by idx 3 > c
        Row row = table.readBy(new Get(idx3, valCol));
        TableTest.verifyColumn(row, valCol, valC);
      }
    });
  }

}
