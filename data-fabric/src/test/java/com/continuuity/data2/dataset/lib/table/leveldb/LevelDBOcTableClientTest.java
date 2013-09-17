package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * test for LevelDB tables.
 */
public class LevelDBOcTableClientTest extends BufferingOcTableClientTest<LevelDBOcTableClient> {

  static LevelDBOcTableService service;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  static Injector getInjector(String dataDir) {
    CConfiguration conf = CConfiguration.create();
    if (dataDir != null) {
      conf.set(Constants.CFG_DATA_LEVELDB_DIR, dataDir);
    } else {
      conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    }
    return Guice.createInjector(new DataFabricLevelDBModule(conf));
  }

  @BeforeClass
  public static void init() throws Exception {
    Injector injector = getInjector(null);
    service = injector.getInstance(LevelDBOcTableService.class);
  }

  @Override
  protected LevelDBOcTableClient getTable(String name) throws IOException {
    return new LevelDBOcTableClient(name, service);
  }

  @Override
  protected DataSetManager getTableManager() throws IOException {
    return new LevelDBOcTableManager(service);
  }

  @Test
  public void testListTablesAcrossRestart() throws Exception {
    // todo make this test run for other in-memory and hbase - requires refactoring of their injection
    final File levelDBPath = tempFolder.newFolder();
    // test on ASCII table name but also on some non-ASCII ones
    final String[] tableNames = { "table", "t able", "t\u00C3ble", "100%" };

    // create an injector and get the ds manager
    Injector injector = getInjector(levelDBPath.getAbsolutePath());
    DataSetAccessor accessor = injector.getInstance(DataSetAccessor.class);
    DataSetManager manager = accessor.getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
    // create a table and verify it is in the list of tables
    for (String tableName : tableNames) {
      manager.create(tableName);
      Assert.assertTrue(accessor.list(DataSetAccessor.Namespace.USER).containsKey(tableName));
    }
    // create an new injector (to simulate single node restart
    injector = getInjector(levelDBPath.getAbsolutePath());
    accessor = injector.getInstance(DataSetAccessor.class);
    for (String tableName : tableNames) {
      Assert.assertTrue(accessor.list(DataSetAccessor.Namespace.USER).containsKey(tableName));
    }
  }

}
