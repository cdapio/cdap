package com.continuuity.api.data.dataset;

import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.data2.transaction.TransactionContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for Index table.
 */
public class IndexedTableTest extends DataSetTestBase {

  static IndexedTable table;

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

  static byte[][] colIdxVal = { idxCol, valCol };

  @BeforeClass
  public static void configure() throws Exception {
    setupInstantiator(new IndexedTable("tab", idxCol));
    table = instantiator.getDataSet("tab");
  }

  @Test
  public void testIndexedOperations() throws Exception {

    Row row;

    // start a new transaction
    TransactionContext txContext = newTransaction();
    // add a value c with idx = 1, and b with idx = 2
    table.put(new Put(keyC).add(idxCol, idx1).add(valCol, valC));
    table.put(new Put(keyB).add(idxCol, idx2).add(valCol, valB));
    // commit the transaction
    commitTransaction(txContext);

    txContext = newTransaction();
    // read by key c
    row = table.get(new Get(keyC, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
    // read by key b
    row = table.get(new Get(keyB, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][] { idx2, valB });
    // read by idx 1 -> c
    row = table.readBy(new Get(idx1, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][] { idx1, valC });
    // read by idx 2 -> b
    row = table.readBy(new Get(idx2, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][] { idx2, valB });
    commitTransaction(txContext);

    // start a new transaction
    txContext = newTransaction();
    // add a value a with idx = 1
    table.put(new Put(keyA).add(idxCol, idx1).add(valCol, valA));
    // commit the transaction
    commitTransaction(txContext);

    // read by idx 1 -> a
    txContext = newTransaction();
    row = table.readBy(new Get(idx1, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valA});
    commitTransaction(txContext);

    // start a new transaction
    txContext = newTransaction();
    // delete value a
    table.delete(new Delete(keyA, colIdxVal));
    // commit the transaction
    commitTransaction(txContext);

    // read by idx 1 -> c
    txContext = newTransaction();
    row = table.readBy(new Get(idx1, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx1, valC});
    commitTransaction(txContext);

    // start a new transaction
    txContext = newTransaction();
    // add a value aa with idx 2
    table.put(new Put(keyAA).add(idxCol, idx2).add(valCol, valAA));
    // commit the transaction
    commitTransaction(txContext);

    // read by idx 2 -> aa
    txContext = newTransaction();
    row = table.readBy(new Get(idx2, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][] { idx2, valAA });
    commitTransaction(txContext);

    // start a new transaction
    txContext = newTransaction();
    // swap value for aa to ab
    Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valAA, valAB));
    // commit the transaction
    commitTransaction(txContext);

    // read by idx 2 -> ab
    txContext = newTransaction();
    row = table.readBy(new Get(idx2, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][] { idx2, valAB });
    commitTransaction(txContext);

    // start a new transaction
    txContext = newTransaction();
    // swap value for aa to bb
    Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valAB, valBB));
    // commit the transaction
    commitTransaction(txContext);

    // read by idx 2 -> bb (value of key aa)
    txContext = newTransaction();
    row = table.readBy(new Get(idx2, colIdxVal));
    TableTest.verifyColumns(row, colIdxVal, new byte[][]{idx2, valBB});
    commitTransaction(txContext);

    // start a new transaction
    txContext = newTransaction();
    // swap value for aa to null
    Assert.assertTrue(table.compareAndSwap(keyAA, valCol, valBB, null));
    // commit the transaction
    commitTransaction(txContext);

    // read by idx 2 -> null (value of b)
    txContext = newTransaction();
    row = table.readBy(new Get(idx2, colIdxVal));
    TableTest.verifyColumn(row, idxCol, idx2);
    commitTransaction(txContext);

    // start a new transaction
    txContext = newTransaction();
    // swap idx for c to 3
    Assert.assertTrue(table.compareAndSwap(keyC, idxCol, idx1, idx3));
    // commit the transaction
    commitTransaction(txContext);

    // read by idx 1 -> null (no row has that any more)
    txContext = newTransaction();
    Assert.assertNull(table.readBy(new Get(idx1, colIdxVal)).get(idx2));
    // read by idx 3 > c
    row = table.readBy(new Get(idx3, valCol));
    TableTest.verifyColumn(row, valCol, valC);
    commitTransaction(txContext);
  }

}
