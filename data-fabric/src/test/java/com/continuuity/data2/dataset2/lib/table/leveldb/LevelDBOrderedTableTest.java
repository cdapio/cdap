package com.continuuity.data2.dataset2.lib.table.leveldb;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.table.ConflictDetection;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.dataset2.lib.table.BufferingOrederedTableTest;
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
public class LevelDBOrderedTableTest extends BufferingOrederedTableTest<LevelDBOrderedTable> {

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
      new TransactionMetricsModule());
    service = injector.getInstance(LevelDBOcTableService.class);
  }

  @Override
  protected LevelDBOrderedTable getTable(String name, ConflictDetection level) throws IOException {
    return new LevelDBOrderedTable(name, service, level);
  }

  @Override
  protected LevelDBOrderedTableAdmin getTableAdmin(String name) throws IOException {
    DatasetSpecification spec =
      new LevelDBOrderedTableDefinition("foo").configure(name, DatasetProperties.EMPTY);
    return new LevelDBOrderedTableAdmin(spec, service);
  }

  @Test
  public void testTablesSurviveAcrossRestart() throws Exception {
    // todo make this test run for hbase, too - requires refactoring of their injection
    // test on ASCII table name but also on some non-ASCII ones
    final String[] tableNames = { "table", "t able", "t\u00C3ble", "100%" };

    // create a table and verify it is in the list of tables
    for (String tableName : tableNames) {
      LevelDBOrderedTableAdmin admin = getTableAdmin(tableName);
      admin.create();
      Assert.assertTrue(admin.exists());
    }

    // clearing in-mem cache - mimicing JVM restart
    service.clearTables();
    for (String tableName : tableNames) {
      service.list().contains(tableName);
    }
  }

}
