package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.LocalDataSetAccessor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * test for LevelDB tables.
 */
public class LevelDBOcTableClientTest extends BufferingOcTableClientTest<LevelDBOcTableClient> {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  static LevelDBOcTableService service;
  static Injector injector = null;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    injector = Guice.createInjector(
      new ConfigModule(conf),
      new LocationRuntimeModule().getSingleNodeModules(),
      new DataFabricLevelDBModule(),
      new TransactionMetricsModule()
    );
    service = injector.getInstance(LevelDBOcTableService.class);
  }

  @Override
  protected LevelDBOcTableClient getTable(String name, ConflictDetection level) throws IOException {
    return new LevelDBOcTableClient(name, level, service);
  }

  @Override
  protected DataSetManager getTableManager() throws IOException {
    return new LevelDBOcTableManager(service);
  }

  @Test
  public void testListTablesAcrossRestart() throws Exception {
    // todo make this test run for hbase, too - requires refactoring of their injection
    // test on ASCII table name but also on some non-ASCII ones
    final String[] tableNames = { "table", "t able", "t\u00C3ble", "100%" };

    DataSetAccessor accessor = injector.getInstance(DataSetAccessor.class);
    DataSetManager manager = accessor.getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
    // create a table and verify it is in the list of tables
    for (String tableName : tableNames) {
      manager.create(tableName);
      Assert.assertTrue(accessor.list(DataSetAccessor.Namespace.USER).containsKey(tableName));
    }

    // create an new instance of the table service
    LevelDBOcTableService newService = new LevelDBOcTableService();
    newService.setConfiguration(injector.getInstance(CConfiguration.class));
    newService.clearTables();
    LocalDataSetAccessor newAccessor = new LocalDataSetAccessor(injector.getInstance(CConfiguration.class), newService);
    for (String tableName : tableNames) {
      Assert.assertTrue(newAccessor.list(DataSetAccessor.Namespace.USER).containsKey(tableName));
    }
  }

}
