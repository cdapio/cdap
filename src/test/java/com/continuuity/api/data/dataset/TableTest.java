package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.data.operation.StatusCode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;

public class TableTest extends DataSetTestBase {

  static Table table;

  static final byte[] key1 = Bytes.toBytes("key1");
  static final byte[] key2 = Bytes.toBytes("key2");
  static final byte[] key3 = Bytes.toBytes("key3");
  static final byte[] key4 = Bytes.toBytes("key4");
  static final byte[] col1 = Bytes.toBytes("col1");
  static final byte[] col2 = Bytes.toBytes("col2");
  static final byte[] col3 = Bytes.toBytes("col3");
  static final byte[] val1 = Bytes.toBytes("val1");
  static final byte[] val2 = Bytes.toBytes("val2");
  static final byte[] val3 = Bytes.toBytes("val3");

  static final byte[][] col12 = { col1, col2 };
  static final byte[][] val12 = { val1, val2 };
  static final byte[][] val13 = { val1, val3 };
  static final byte[][] col23 = { col2, col3 };
  static final byte[][] val22 = { val2, val2 };
  static final byte[][] val23 = { val2, val3 };
  static final byte[][] col123 = { col1, col2, col3 };
  static final byte[][] val123 = { val1, val2, val3 };

  static final byte[] c = { 'c' }, v = { 'v' };

  @BeforeClass
  public static void configure() throws Exception {
    DataSet kv = new Table("test");
    DataSet t1 = new Table("t1");
    DataSet t2 = new Table("t2");
    DataSet t3 = new Table("t3");
    DataSet t4 = new Table("t4");
    DataSet tBatch = new Table("tBatch");
    setupInstantiator(Lists.newArrayList(kv, t1, t2, t3, t4, tBatch));
    table = instantiator.getDataSet("test");
  }

  public static void verifyColumns(OperationResult<Map<byte[], byte[]>> result,
                                   byte[][] columns, byte[][] expected) {
    Assert.assertEquals(columns.length, expected.length);
    Assert.assertFalse(result.isEmpty());
    Assert.assertNotNull(result.getValue());
    Assert.assertEquals(columns.length, result.getValue().size());
    for (int i = 0; i < columns.length; i++) {
      Assert.assertArrayEquals(expected[i], result.getValue().get(columns[i]));
    }
  }

  public static void verifyColumn(OperationResult<Map<byte[], byte[]>> result,
                                  byte[] column, byte[] expected) {
    verifyColumns(result, new byte[][]{column}, new byte[][]{expected});
  }

  void verifyColumns(OperationResult<Map<byte[], byte[]>> result,
                    byte[][] columns, long[] expected) {
    byte[][] expectedBytes = new byte[expected.length][];
    for (int i = 0; i < expected.length; i++) {
      expectedBytes[i] = Bytes.toBytes(expected[i]);
    }
    verifyColumns(result, columns, expectedBytes);
  }

  public static void verifyColumn(OperationResult<Map<byte[], byte[]>> result,
                                  byte[] column, long expected) {
    verifyColumn(result, column, Bytes.toBytes(expected));
  }

  public static void verifyNull(OperationResult<Map<byte[], byte[]>> result,
                                byte[] column) {
    verifyNull(result, new byte[][] { column });
  }

  public static void verifyNull(OperationResult<Map<byte[], byte[]>> result,
                                byte[][] columns) {
    Assert.assertTrue(columns.length > 0);
    Assert.assertTrue(result.isEmpty());
  }

  private byte[][] makeArray(String prefix, Integer ... numbers) {
    byte[][] array = new byte[numbers.length][];
    int idx = 0;
    for (int i : numbers) {
      array[idx++] = (prefix + i).getBytes();
    }
    return array;
  }
  private byte[][] makeColumns(Integer ... numbers) {
    return makeArray("c", numbers);
  }
  private byte[][] makeValues(Integer ... numbers) {
    return makeArray("v", numbers);
  }

  // attempt to compare and swap, expect failure
  void attemptSwap(Table tab, Swap swap) {
    try {
      tab.write(swap);
      Assert.fail("swap should have failed");
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.WRITE_CONFLICT, e.getStatus());
    }

  }

  @Test
  public void testSyncWriteReadSwapDelete() throws OperationException {

    // this test runs all operations synchronously
    newTransaction(Mode.Sync);

    OperationResult<Map<byte[], byte[]>> result;

    // write a value and read it back
    table.write(new Write(key1, col1, val2));
    result = table.read(new Read(key1, col1));
    verifyColumn(result, col1, val2);

    // update the value, and add two new columns, and read them back
    table.write(new Write(key1, col123, val123));
    // read explicitly all three columns
    result = table.read(new Read(key1, col123));
    verifyColumns(result, col123, val123);
    // read the range of all columns (start = stop = null)
    result = table.read(new Read(key1, null, null));
    verifyColumns(result, col123, val123);
    // read the range up to (but excluding) col2_ -> col1, col2
    result = table.read(new Read(key1, null, Bytes.toBytes("col2_")));
    verifyColumns(result, col12, val12);
    // read the range from col1 up to (but excluding) col3 -> col1, col2
    result = table.read(new Read(key1, col1, col3));
    verifyColumns(result, col12, val12);
    // read the range from col2 up to (but excluding) col3_ -> col2, col3
    result = table.read(new Read(key1, col2, Bytes.toBytes("col3_")));
    verifyColumns(result, col23, val23);
    // read the range from col2 on -> col2, col3
    result = table.read(new Read(key1, col2, null));
    verifyColumns(result, col23, val23);
    // read all columns
    result = table.read(new Read(key1));
    verifyColumns(result, col123, val123);
    // read the first 2 columns
    result = table.read(new Read(key1, 2));
    verifyColumns(result, col12, val12);

    // delete one column, verify that it is gone
    table.write(new Delete(key1, col1));
    result = table.read(new Read(key1, null, null));
    verifyColumns(result, col23, val23);

    // attempt to compare and swap, should fail because col2==val3
    attemptSwap(table, new Swap(key1, col2, val3, val1));
    // attempt to compare and swap, should fail because col1 is deleted
    attemptSwap(table, new Swap(key1, col1, val3, val1));
    // attempt to compare with null and swap, should fail because col2 exists
    attemptSwap(table, new Swap(key1, col2, null, val1));

    // compare and swap one column with a new value
    table.write(new Swap(key1, col2, val2, val1));
    result = table.read(new Read(key1, null, null));
    verifyColumns(result, col23, val13);

    // compare and swap one column with null
    table.write(new Swap(key1, col2, val1, null));
    result = table.read(new Read(key1, col2));
    verifyNull(result, col2);

    // compare and swap a null column with a new value
    table.write(new Swap(key1, col2, null, val2));
    result = table.read(new Read(key1, col2));
    verifyColumn(result, col2, val2);
  }



  @Test
  public void testIncrement() throws OperationException {

    // this test runs all operations synchronously
    newTransaction(Mode.Sync);

    OperationResult<Map<byte[], byte[]>> result;

    // verify that there is no value when we start
    result = table.read(new Read(key2, col1));
    verifyNull(result, col1);
    result = table.read(new Read(key2, col2));
    verifyNull(result, col1);

    // increment one value
    table.write(new Increment(key2, col1, 5L));
    result = table.read(new Read(key2, col1));
    verifyColumn(result, col1, 5L);

    // increment two values
    table.write(new Increment(key2, col12, new long[]{2L, 3L}));
    result = table.read(new Read(key2, col12));
    verifyColumns(result, col12, new long[] { 7L, 3L });

    // write a val < 8-bytes to a column, then try to increment it -> fail
    table.write(new Write(key2, col3, val3));
    try {
      table.write(new Increment(key2, col3, 1L));
      Assert.fail("increment of 'val3' should have failed (not a long)" );
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.ILLEGAL_INCREMENT, e.getStatus());
    }
  }

  @Test
  public void testASyncWriteReadSwapDelete() throws OperationException {

    OperationResult<Map<byte[], byte[]>> result;

    // defers all writes to the transaction commit
    newTransaction(Mode.Batch);

    // write three columns of one row
    table.write(new Write(key3, col123, val123));
    // increment another column of another row
    table.write(new Increment(key4, col1, 1L));
    // verify not there
    result = table.read(new Read(key3, null, null));
    verifyNull(result, col123);
    result = table.read(new Read(key4, null, null));
    verifyNull(result, col1);

    // commit xaction
    commitTransaction();

    // verify all are there with sync reads
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col123, val123);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 1L);

    // start a new transaction
    newTransaction(Mode.Batch);
    // increment same column again
    table.write(new Increment(key4, col1, 1L));
    // delete one column
    table.write(new Delete(key3, col3));
    // swap one of the other columns
    table.write(new Swap(key3, col1, val1, val2));
    // verify not executed yet
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col123, val123);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 1L);

    // commit xaction
    commitTransaction();

    // verify all are there with sync reads
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col12, val22);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 2L);

    // start a new transaction
    newTransaction(Mode.Batch);
    // increment same column again
    table.write(new Increment(key4, col1, 1L));
    // delete another column
    table.write(new Delete(key3, col2));
    // swap the remaining column with wrong value
    table.write(new Swap(key3, col1, val1, val2));
    // verify not executed yet
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col12, val22);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 2L);
    // commit xaction, should fail
    try {
      commitTransaction();
      Assert.fail("transaction should have failed due to swap");
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.WRITE_CONFLICT, e.getStatus());
    }
    // verify none was executed
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col12, val22);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 2L);
  }

  @Test
  public void testWriteReadSwapDelete() throws OperationException {

    Table table = instantiator.getDataSet("t3");
    OperationResult<Map<byte[], byte[]>> result;

    // defer writes until commit or a read is performed
    newTransaction(Mode.Smart);

    // write three columns of one row
    table.write(new Write(key3, col123, val123));
    // increment another column of another row
    table.write(new Increment(key4, col1, 1L));
    // verify they are visible in the transaction
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col123, val123);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 1L);

    // commit xaction
    commitTransaction();

    // verify all are there with sync reads
    newTransaction(Mode.Sync);
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col123, val123);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 1L);
    commitTransaction();

    // start a new transaction
    newTransaction(Mode.Smart);
    // increment same column again
    table.write(new Increment(key4, col1, 1L));
    // delete one column
    table.write(new Delete(key3, col3));
    // swap one of the other columns
    table.write(new Swap(key3, col1, val1, val2));
    // verify writes are visible in the transaction
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col12, val22);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 2L);

    // commit xaction
    commitTransaction();

    // verify all are there with sync reads
    newTransaction(Mode.Sync);
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col12, val22);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 2L);
    commitTransaction();

    // start a new transaction
    newTransaction(Mode.Smart);
    // increment same column again
    table.write(new Increment(key4, col1, 1L));
    // delete another column
    table.write(new Delete(key3, col2));
    // verify writes are visible in transaction
    result = table.read(new Read(key3, null, null));
    verifyColumn(result, col1, val2);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 3L);

    // swap the remaining column with wrong value
    table.write(new Swap(key3, col1, val1, val2));
    // attempt to read the resulting value
    try {
      table.read(new Read(key3, null, null));
      Assert.fail("transaction should have failed due to swap");
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.WRITE_CONFLICT, e.getStatus());
    }
    // verify none was committed with sync reads
    newTransaction(Mode.Sync);
    result = table.read(new Read(key3, null, null));
    verifyColumns(result, col12, val22);
    result = table.read(new Read(key4, null, null));
    verifyColumn(result, col1, 2L);
  }

  @Test
  public void testTransactionAcrossTables() throws Exception {
    Table table1 = instantiator.getDataSet("t1");
    Table table2 = instantiator.getDataSet("t2");

    OperationResult<Map<byte[], byte[]>> result;

    // initialize the table with sync operations
    newTransaction(Mode.Sync);

    // write a value to table1 and verify it
    table1.write(new Write(key1, col1, val1));
    result = table1.read(new Read(key1, col1));
    verifyColumn(result, col1, val1);
    // increment a column in table2 and verify it
    table2.write(new Increment(key2, col2, 5L));
    result = table2.read(new Read(key2, col2));
    verifyColumn(result, col2, 5L);
    // write a value to another row in table2 and verify it
    table2.write(new Write(key3, col3, val3));
    result = table2.read(new Read(key3, col3));
    verifyColumn(result, col3, val3);

    // start a new transaction
    newTransaction(Mode.Batch);
    // add a write for table 1 to the transaction
    table1.write(new Write(key1, col1, val2));
    // verify that the write is not effective yet
    result = table1.read(new Read(key1, col1));
    verifyColumn(result, col1, val1);
    // add an increment for the same column in table 2
    table2.write(new Increment(key2, col2, 3L));
    // verify that the increment is not effective yet
    result = table2.read(new Read(key2, col2));
    verifyColumn(result, col2, 5L);
    // submit a delete for table 2
    table2.write(new Delete(key3, col3));
    // verify that the delete is not effective yet
    result = table2.read(new Read(key3, col3));
    verifyColumn(result, col3, val3);
    // add a swap for a third table that should fail
    table.write(new Swap(Bytes.toBytes("non-exist"), col1, val1, val1));

    // attempt to commit the transaction, should fail
    try {
      commitTransaction();
      Assert.fail("swap should have failed");
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.WRITE_CONFLICT, e.getStatus());
    }

    // verify old value are still there, synchronously
    newTransaction(Mode.Sync);

    result = table1.read(new Read(key1, col1));
    verifyColumn(result, col1, val1);
    result = table2.read(new Read(key2, col2));
    verifyColumn(result, col2, 5L);
    result = table2.read(new Read(key3, col3));
    verifyColumn(result, col3, val3);
  }

  @Test
  public void testTableWithoutDelegateCantOperate() throws OperationException {
    Table t = new Table("xyz");
    try {
      t.read(new Read(key1, col1));
      Assert.fail("Read should throw an exception when called before runtime");
    } catch (IllegalStateException e) {
      // expected
    }
    try {
      t.write(new Write(key1, col1, val1));
      Assert.fail("Write should throw an exception when called before runtime");
    } catch (IllegalStateException e) {
      // expected
    }
    try {
      t.write(new Write(key1, col1, val1));
      Assert.fail("Stage should throw an exception when called before " +
          "runtime");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testColumnRange() throws OperationException {
    Table table = instantiator.getDataSet("t4");
    // start a transaction
    newTransaction(Mode.Sync);

    // write a row with 10 columns
    byte[][] allColumns = makeColumns(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    byte[][] allValues =  makeValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    table.write(new Write(key1, allColumns, allValues));

    // read a column range of all columns
    OperationResult<Map<byte[], byte[]>> result;
    result = table.read(new Read(key1, null, null));
    verifyColumns(result, allColumns, allValues);

    // read a column range from 7 to the end
    result = table.read(new Read(key1, allColumns[7], null));
    verifyColumns(result, makeColumns(7, 8, 9), makeValues(7, 8, 9));

    // read a column range from the beginning to exclusive) 2, that is inclusive 1
    result = table.read(new Read(key1, null, allColumns[2]));
    verifyColumns(result, makeColumns(0, 1), makeValues(0, 1));

    // read a column range with limit
    result = table.read(new Read(key1, allColumns[2], null, 4));
    verifyColumns(result, makeColumns(2, 3, 4, 5), makeValues(2, 3, 4, 5));

    // read a column range with limit that return less then limit
    result = table.read(new Read(key1, allColumns[8], null, 4));
    verifyColumns(result, makeColumns(8, 9), makeValues(8, 9));
  }

  @Test
  public void testBatchReads() throws OperationException, InterruptedException {
    Table t = instantiator.getDataSet("tBatch");

    // start a transaction
    newTransaction(Mode.Smart);
    // write 1000 random values to the table and remember them in a set
    SortedSet<Long> keysWritten = Sets.newTreeSet();
    Random rand = new Random(451);
    for (int i = 0; i < 1000; i++) {
      long keyLong = rand.nextLong();
      byte[] key = Bytes.toBytes(keyLong);
      t.write(new Write(key, new byte[][]{c, key}, new byte[][]{key, v}));
      keysWritten.add(keyLong);
    }
    // commit transaction
    commitTransaction();

    // start a sync transaction
    newTransaction(Mode.Sync);
    // get the splits for the table
    List<Split> splits = t.getSplits();
    // read each split and verify the keys
    SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten);
    verifySplits(t, splits, keysToVerify);

    // start a sync transaction
    newTransaction(Mode.Sync);
    // get specific number of splits for a subrange
    keysToVerify = Sets.newTreeSet(keysWritten.subSet(0x10000000L, 0x40000000L));
    splits = t.getSplits(5, Bytes.toBytes(0x10000000L), Bytes.toBytes(0x40000000L));
    Assert.assertTrue(splits.size() <= 5);
    // read each split and verify the keys
    verifySplits(t, splits, keysToVerify);
  }

  // helper to verify that the split readers for the given splits return exactly a set of keys
  private void verifySplits(Table t, List<Split> splits, SortedSet<Long> keysToVerify)
    throws OperationException, InterruptedException {
    // read each split and verify the keys, remove all read keys from the set
    for (Split split : splits) {
      SplitReader<byte[], Map<byte[], byte[]>> reader = t.createSplitReader(split);
      reader.initialize(split);
      while (reader.nextKeyValue()) {
        byte[] key = reader.getCurrentKey();
        Map<byte[], byte[]> row = reader.getCurrentValue();
        // verify each row has the two columns written
        Assert.assertArrayEquals(key, row.get(c));
        Assert.assertArrayEquals(v, row.get(key));
        Assert.assertTrue(keysToVerify.remove(Bytes.toLong(key)));
      }
    }
    // verify all keys have been read
    if (!keysToVerify.isEmpty()) {
      System.out.println("Remaining [" + keysToVerify.size() + "]: " + keysToVerify);
    }
    Assert.assertTrue(keysToVerify.isEmpty());
  }
}
