package com.continuuity.api.dataset.lib;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.data2.dataset2.AbstractDatasetTest;
import com.continuuity.data2.dataset2.lib.table.CoreDatasetsModule;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;

/**
 * Key value table test.
 */
public class KeyValueTableTest extends AbstractDatasetTest {

  static final byte[] KEY1 = Bytes.toBytes("KEY1");
  static final byte[] KEY2 = Bytes.toBytes("KEY2");
  static final byte[] KEY3 = Bytes.toBytes("KEY3");
  static final byte[] VAL1 = Bytes.toBytes("VAL1");
  static final byte[] VAL2 = Bytes.toBytes("VAL2");
  static final byte[] VAL3 = Bytes.toBytes("VAL3");

  private KeyValueTable kvTable;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    addModule("core", new CoreDatasetsModule());
    createInstance("keyValueTable", "test", DatasetProperties.EMPTY);
    kvTable = getInstance("test");
  }

  @After
  public void tearDown() throws Exception {
    deleteModule("core");
    super.tearDown();
  }

  @Test
  public void testSyncWriteReadSwapDelete() throws Exception {
    TransactionExecutor txnl = newTransactionExecutor(kvTable);

    // this test runs all operations synchronously
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write a value and read it back
        kvTable.write(KEY1, VAL1);
        Assert.assertArrayEquals(VAL1, kvTable.read(KEY1));

        // update the value and read it back
        kvTable.write(KEY1, VAL2);
        Assert.assertArrayEquals(VAL2, kvTable.read(KEY1));

        // attempt to swap, expecting old value
        Assert.assertFalse(kvTable.swap(KEY1, VAL1, VAL3));
        Assert.assertArrayEquals(VAL2, kvTable.read(KEY1));

        // swap the value and read it back
        Assert.assertTrue(kvTable.swap(KEY1, VAL2, VAL3));
        Assert.assertArrayEquals(VAL3, kvTable.read(KEY1));

        // delete the value and verify its gone
        kvTable.delete(KEY1);
        Assert.assertNull(kvTable.read(KEY1));
      }
    });
  }

  @Test
  public void testASyncWriteReadSwapDelete() throws Exception {
    TransactionExecutor txnl = newTransactionExecutor(kvTable);

    // defer all writes until commit
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write a value
        kvTable.write(KEY2, VAL1);
      }
    });

    // verify synchronously
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // verify that the value is now visible
        Assert.assertArrayEquals(VAL1, kvTable.read(KEY2));
      }
    });

    // defer all writes until commit
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // update the value
        kvTable.write(KEY2, VAL2);
      }
    });

    // verify synchronously
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // verify that the value is now visible
        Assert.assertArrayEquals(VAL2, kvTable.read(KEY2));
      }
    });

    // defer all writes until commit
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write a swap, this should fail
        Assert.assertFalse(kvTable.swap(KEY2, VAL1, VAL3));
        Assert.assertArrayEquals(VAL2, kvTable.read(KEY2));
      }
    });

    // defer all writes until commit
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // swap the value
        Assert.assertTrue(kvTable.swap(KEY2, VAL2, VAL3));
      }
    });

    // verify synchronously
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // verify the value was swapped
        Assert.assertArrayEquals(VAL3, kvTable.read(KEY2));
      }
    });

    // defer all writes until commit
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // delete the value
        kvTable.delete(KEY2);
      }
    });

    // verify synchronously
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // verify it is gone now
        Assert.assertNull(kvTable.read(KEY2));
      }
    });
  }

  @Test
  public void testTransactionAcrossTables() throws Exception {
    createInstance("keyValueTable", "t1", DatasetProperties.EMPTY);
    createInstance("keyValueTable", "t2", DatasetProperties.EMPTY);

    final KeyValueTable table1 = getInstance("t1");
    final KeyValueTable table2 = getInstance("t2");
    TransactionExecutor txnl = newTransactionExecutor(table1, table2);

    // write a value to table1 and verify it
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table1.write(KEY1, VAL1);
        Assert.assertArrayEquals(VAL1, table1.read(KEY1));
        table2.write(KEY2, VAL2);
        Assert.assertArrayEquals(VAL2, table2.read(KEY2));
      }
    });

    // start a new transaction
    try {
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          // add a write for table 1 to the transaction
          table1.write(KEY1, VAL2);
          // submit a delete for table 2
          table2.delete(KEY2);
          throw new RuntimeException("Cancel transaction");
        }
      });
      Assert.fail("Transaction should have been cancelled");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("Cancel transaction", e.getCause().getMessage());
    }

    // add a swap for a third table that should fail
    Assert.assertFalse(kvTable.swap(KEY3, VAL1, VAL1));

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertArrayEquals(VAL1, table1.read(KEY1));
        Assert.assertArrayEquals(VAL2, table2.read(KEY2));
      }
    });

    // verify synchronously that old value are still there
    final KeyValueTable table1v2 = getInstance("t1");
    final KeyValueTable table2v2 = getInstance("t2");
    TransactionExecutor txnlv2 = newTransactionExecutor(table1v2, table2v2);
    txnlv2.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertArrayEquals(VAL1, table1v2.read(KEY1));
        Assert.assertArrayEquals(VAL2, table2v2.read(KEY2));
      }
    });

    deleteInstance("t1");
    deleteInstance("t2");
  }

  @Test
  public void testBatchReads() throws Exception {
    createInstance("keyValueTable", "tBatch", DatasetProperties.EMPTY);

    final KeyValueTable t = getInstance("tBatch");
    TransactionExecutor txnl = newTransactionExecutor(t);

    final SortedSet<Long> keysWritten = Sets.newTreeSet();

    // start a transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write 1000 random values to the table and remember them in a set
        Random rand = new Random(451);
        for (int i = 0; i < 1000; i++) {
          long keyLong = rand.nextLong();
          byte[] key = Bytes.toBytes(keyLong);
          t.write(key, key);
          keysWritten.add(keyLong);
        }
      }
    });

    // start a sync transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // get the splits for the table
        List<Split> splits = t.getSplits();
        // read each split and verify the keys
        SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten);
        verifySplits(t, splits, keysToVerify);
      }
    });

    // start a sync transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // get specific number of splits for a subrange
        SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten.subSet(0x10000000L, 0x40000000L));
        List<Split> splits = t.getSplits(5, Bytes.toBytes(0x10000000L), Bytes.toBytes(0x40000000L));
        Assert.assertTrue(splits.size() <= 5);
        // read each split and verify the keys
        verifySplits(t, splits, keysToVerify);
      }
    });

    deleteInstance("tBatch");
  }

  // helper to verify that the split readers for the given splits return exactly a set of keys
  private void verifySplits(KeyValueTable t, List<Split> splits, SortedSet<Long> keysToVerify)
    throws InterruptedException {
    // read each split and verify the keys, remove all read keys from the set
    for (Split split : splits) {
      SplitReader<byte[], byte[]> reader = t.createSplitReader(split);
      reader.initialize(split);
      while (reader.nextKeyValue()) {
        byte[] key = reader.getCurrentKey();
        byte[] value = reader.getCurrentValue();
        // verify each row has the two columns written
        Assert.assertArrayEquals(key, value);
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
