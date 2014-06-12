package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClient;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public abstract class NamespacingDataSetAccessorTest {
  protected abstract DataSetAccessor getDataSetAccessor();

  static CConfiguration conf = CConfiguration.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf.set(DataSetAccessor.CFG_TABLE_PREFIX, "test");
  }

  @Test
  public void testDataSetNames() throws Exception {
    // User table
    DataSetManager dsManager =
      getDataSetAccessor().getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
    Assert.assertFalse(dsManager.exists("myTable"));
    dsManager.create("myTable");
    Assert.assertTrue(dsManager.exists("myTable"));

    OrderedColumnarTable myTable =
      getDataSetAccessor().getDataSetClient("myTable", OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);

    Assert.assertTrue(
      getRawName((DataSetClient) myTable).startsWith("test." + DataSetAccessor.Namespace.USER.getName()));

    // System table
    dsManager =
      getDataSetAccessor().getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM);
    Assert.assertFalse(dsManager.exists("myTable"));
    dsManager.create("myTable");
    Assert.assertTrue(dsManager.exists("myTable"));

    myTable =
      getDataSetAccessor().getDataSetClient("myTable", OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM);

    Assert.assertTrue(
      getRawName((DataSetClient) myTable).startsWith("test." + DataSetAccessor.Namespace.SYSTEM.getName()));
  }

  private String getRawName(DataSetClient dsClient) {
    if (dsClient instanceof OrderedColumnarTable) {
      return ((BufferingOcTableClient) dsClient).getTableName();
    }

    throw new RuntimeException("Unknown DataSetClient type: " + dsClient.getClass());
  }
}
