/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset.lib.table.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.DataSetAccessor;
import co.cask.cdap.data.LocalDataSetAccessor;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.dataset.api.DataSetManager;
import co.cask.cdap.data2.dataset.lib.table.BufferingOcTableClientTest;
import co.cask.cdap.data2.dataset.lib.table.ConflictDetection;
import co.cask.cdap.data2.dataset.lib.table.OrderedColumnarTable;
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
