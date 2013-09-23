package com.continuuity.data;

import com.continuuity.common.conf.Constants;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClient;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 *
 */
public class LocalDataSetAccessorTest extends NamespacingDataSetAccessorTest {
  private static DataSetAccessor dsAccessor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NamespacingDataSetAccessorTest.beforeClass();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    Injector injector = Guice.createInjector(new DataFabricLevelDBModule(conf));
    dsAccessor = injector.getInstance(DataSetAccessor.class);
  }

  @Override
  protected DataSetAccessor getDataSetAccessor() {
    return dsAccessor;
  }

  @Override
  protected String getRawName(DataSetClient dsClient) {
    if (dsClient instanceof OrderedColumnarTable) {
      return ((BufferingOcTableClient) dsClient).getTableName();
    }

    throw new RuntimeException("Unknown DataSetClient type: " + dsClient.getClass());
  }
}
