package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

import java.lang.String;
import java.util.Map;

/**
 *
 */
public abstract class OrderedColumnarTableTest {
  private static final byte[] R1 = Bytes.toBytes("r1");

  private static final byte[] C1 = Bytes.toBytes("c1");
  private static final byte[] C2 = Bytes.toBytes("c2");

  private static final byte[] V1 = Bytes.toBytes("v1");

  protected abstract OrderedColumnarTable getTable(String name) throws Exception;
  protected abstract DataSetManager getTableManager() throws Exception;

  @Test
  public void testPutGetNoTx() throws Exception {
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    try {
      OrderedColumnarTable myTable = getTable("myTable");
      myTable.put(R1, $(C1), $(V1));

      verify($(C1, V1), myTable.get(R1, $(C1, C2)));

    } finally {
      manager.drop("myTable");
    }
  }

  private void verify(byte[][] expected, OperationResult<Map<byte[], byte[]>> actual) {
    Preconditions.checkArgument(expected.length % 2 == 0, "expected [key,val] pairs in first param");
    if (expected.length == 0) {
      Assert.assertTrue(actual.isEmpty());
    }
    Assert.assertEquals(expected.length / 2, actual.getValue().size());
    for (int i = 0; i < expected.length; i += 2) {
      byte[] key = expected[i];
      byte[] val = expected[i + 1];
      Assert.assertEquals(val, actual.getValue().get(key));
    }
  }

  private static byte[][] $(byte[]... elems) {
    return elems;
  }
}
