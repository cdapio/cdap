/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.service.NoOpStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LevelDBOrderedTableServiceTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  static LevelDBOrderedTableService service;
  static Injector injector = null;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    injector = Guice.createInjector(
      new ConfigModule(conf),
      new LocationRuntimeModule().getStandaloneModules(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new DataSetsModules().getLocalModule(),
      new DataFabricLevelDBModule(),
      new TransactionMetricsModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StreamMetaStore.class).to(NoOpStreamMetaStore.class);
        }
      });
    service = injector.getInstance(LevelDBOrderedTableService.class);
  }

  @Test
  public void testGetTableStats() throws Exception {
    Assert.assertNull(service.getTableStats().get("table1"));
    Assert.assertNull(service.getTableStats().get("table2"));

    service.ensureTableExists("table1");
    service.ensureTableExists("table2");
    // We sleep to allow ops flush out to disk
    TimeUnit.SECONDS.sleep(1);

    // NOTE: empty table may take non-zero disk space: it stores some meta files as well
    Assert.assertNotNull(service.getTableStats().get("table1"));
    long table1Size = service.getTableStats().get("table1").getDiskSizeBytes();
    Assert.assertNotNull(service.getTableStats().get("table2"));
    long table2Size = service.getTableStats().get("table2").getDiskSizeBytes();

    writeSome("table1");
    TimeUnit.SECONDS.sleep(1);

    long table1SizeUpdated = service.getTableStats().get("table1").getDiskSizeBytes();
    Assert.assertTrue(table1SizeUpdated > table1Size);
    table1Size = table1SizeUpdated;
    Assert.assertEquals(table2Size, service.getTableStats().get("table2").getDiskSizeBytes());

    writeSome("table1");
    writeSome("table2");
    TimeUnit.SECONDS.sleep(1);

    Assert.assertTrue(service.getTableStats().get("table1").getDiskSizeBytes() > table1Size);
    long table2SizeUpdated = service.getTableStats().get("table2").getDiskSizeBytes();
    Assert.assertTrue(table2SizeUpdated > table2Size);
    table2Size = table2SizeUpdated;

    service.dropTable("table1");
    TimeUnit.SECONDS.sleep(1);

    Assert.assertNull(service.getTableStats().get("table1"));
    Assert.assertEquals(table2Size, service.getTableStats().get("table2").getDiskSizeBytes());
  }

  private void writeSome(String tableName) throws IOException {
    LevelDBOrderedTableCore table = new LevelDBOrderedTableCore(tableName, service);
    Random r = new Random();
    byte[] key = new byte[100];
    byte[] value = new byte[1024 * 1024];
    for (int i = 0; i < 8; i++) {
      r.nextBytes(key);
      r.nextBytes(value);
      table.put(key, Bytes.toBytes("column" + i), value, 0L);
    }
  }
}
