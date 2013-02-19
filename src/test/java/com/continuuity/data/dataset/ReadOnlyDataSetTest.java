package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

public class ReadOnlyDataSetTest extends DataSetTestBase {

  static KeyValueTable kvTable;

  @BeforeClass
  public static void configure() throws OperationException {

    // a key value table
    DataSet table = new KeyValueTable("table");
    setupInstantiator(Collections.singletonList(table));

    // set read only mode
    instantiator.setReadOnly();
    newTransaction(Mode.Sync);

    // and get the runtime instance of the table
    kvTable = instantiator.getDataSet("table");
  }

  static final byte[] a = { 'a' };
  static final byte[] b = { 'b' };

  @Test(expected = UnsupportedOperationException.class)
  public void testWriteToReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(a));
    // synchronous write should fail
    kvTable.write(a, b);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testIncrementToReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(new byte[] { 'a' }));
    // asynchronous write should fail
    kvTable.increment(a, 10L);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testIncrementGetToReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(a));
    // asynchronous write should fail
    @SuppressWarnings("unused")
    long value = kvTable.incrementAndGet(a, 10L);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSwapToReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(a));
    // asynchronous write should fail
    kvTable.swap(a, null, b);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteToReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(a));
    // asynchronous write should fail
    kvTable.delete(a);
  }

}