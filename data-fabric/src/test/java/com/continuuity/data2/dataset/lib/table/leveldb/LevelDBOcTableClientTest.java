package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTableTest;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * test for LevelDB tables.
 */
public class LevelDBOcTableClientTest extends OrderedColumnarTableTest {

  static LevelDBOcTableService service;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    Injector injector = Guice.createInjector(new DataFabricLevelDBModule(conf));
    service = injector.getInstance(LevelDBOcTableService.class);
  }

  @Override
  protected OrderedColumnarTable getTable(String name) throws IOException {
    return new LevelDBOcTableClient(name, service);
  }

  @Override
  protected DataSetManager getTableManager() throws IOException {
    return new LevelDBOcTableManager(service);
  }
}
