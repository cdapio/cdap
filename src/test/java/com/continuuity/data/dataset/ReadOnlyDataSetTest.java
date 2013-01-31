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
  public static void configure() {

    // a key value table
    DataSet table = new KeyValueTable("table");
    setupInstantiator(Collections.singletonList(table));

    // set read only mode
    instantiator.setReadOnly();

    // and get the runtime instance of the table
    kvTable = instantiator.getDataSet("table");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSyncWriteToReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(new byte[] { 'a' }));
    // synchronous write should fail
    kvTable.exec(new KeyValueTable.WriteKey(new byte[] { 'a' } , new byte[] { 'b' }));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAsyncWriteToReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(new byte[] { 'a' }));
    // asynchronous write should fail
    kvTable.stage(new KeyValueTable.WriteKey(new byte[] { 'a' } , new byte[] { 'b' }));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testClosureOnReadOnlyTable() throws OperationException {
    // read should work, but since we don't write
    Assert.assertNull(kvTable.read(new byte[] { 'a' }));
    // closure should fail
    kvTable.closure(new KeyValueTable.IncrementKey(new byte[] { 'a' }));
  }

}