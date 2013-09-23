package com.continuuity.data;

import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 */
public class DistributedDataSetAccessorTest extends NamespacingDataSetAccessorTest {
  private static DataSetAccessor dsAccessor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NamespacingDataSetAccessorTest.beforeClass();
    HBaseTestBase.startHBase();
    dsAccessor = new DistributedDataSetAccessor(conf, HBaseTestBase.getConfiguration());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HBaseTestBase.stopHBase();
  }

  @Override
  protected DataSetAccessor getDataSetAccessor() {
    return dsAccessor;
  }

  @Override
  protected String getRawName(DataSetClient dsClient) {
    if (dsClient instanceof OrderedColumnarTable) {
      return ((HBaseOcTableClient) dsClient).getHBaseTableName();
    }

    throw new RuntimeException("Unknown DataSetClient type: " + dsClient.getClass());
  }
}
