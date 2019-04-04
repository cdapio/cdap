/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.data.hbase.HBaseTestBase;
import io.cdap.cdap.data.hbase.HBaseTestFactory;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionMetricsModule;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTableTest;
import io.cdap.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import io.cdap.cdap.test.SlowTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * metrics table test for HBase.
 */
@Category(SlowTests.class)
public class HBaseMetricsTableTest extends MetricsTableTest {

  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  private static HBaseTableUtil tableUtil;
  private static DatasetFramework dsFramework;
  private static HBaseDDLExecutor ddlExecutor;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    Injector injector = Guice.createInjector(new DataFabricModules().getDistributedModules(),
                                             new ConfigModule(cConf, TEST_HBASE.getConfiguration()),
                                             new ZKClientModule(),
                                             new ZKDiscoveryModule(),
                                             new TransactionMetricsModule(),
                                             new DFSLocationModule(),
                                             new NamespaceAdminTestModule(),
                                             new SystemDatasetRuntimeModule().getDistributedModules(),
                                             new DataSetsModules().getInMemoryModules(),
                                             new AuthorizationTestModule(),
                                             new AuthorizationEnforcementModule().getInMemoryModules(),
                                             new AuthenticationContextModules().getNoOpModule(),
                                             new AbstractModule() {
                                               @Override
                                               protected void configure() {
                                                 bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                                                 bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
                                               }
                                             });

    dsFramework = injector.getInstance(DatasetFramework.class);
    tableUtil = injector.getInstance(HBaseTableUtil.class);
    ddlExecutor = new HBaseDDLExecutorFactory(cConf, TEST_HBASE.getHBaseAdmin().getConfiguration()).get();
    ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tableUtil.deleteAllInNamespace(ddlExecutor, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM),
                                   TEST_HBASE.getHBaseAdmin().getConfiguration());
    ddlExecutor.deleteNamespaceIfExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
  }

  @Override
  @Test
  public void testConcurrentIncrement() throws Exception {
    String testConcurrentIncrement = "testConcurrentIncrement";
    final MetricsTable table = getTable(testConcurrentIncrement);
    final int rounds = 500;
    Map<byte[], Long> inc1 = ImmutableMap.of(X, 1L, Y, 2L);
    Map<byte[], Long> inc2 = ImmutableMap.of(Y, 1L, Z, 2L);
    // HTable used by HBaseMetricsTable is not thread safe, so each thread must use a separate instance
    // HBaseMetricsTable does not support mixed increment and incrementAndGet so the
    // updates and assertions here are different from MetricsTableTest.testConcurrentIncrement()
    Collection<? extends Thread> threads =
      ImmutableList.of(new IncThread(getTable(testConcurrentIncrement), A, inc1, rounds),
                       new IncThread(getTable(testConcurrentIncrement), A, inc2, rounds),
                       new IncAndGetThread(getTable(testConcurrentIncrement), A, R, 5, rounds),
                       new IncAndGetThread(getTable(testConcurrentIncrement), A, R, 2, rounds));
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
      if (t instanceof Closeable) {
        ((Closeable) t).close();
      }
    }
    assertEquals(rounds, Bytes.toLong(table.get(A, X)));
    assertEquals(3 * rounds, Bytes.toLong(table.get(A, Y)));
    assertEquals(2 * rounds, Bytes.toLong(table.get(A, Z)));
    assertEquals(7 * rounds, Bytes.toLong(table.get(A, R)));
  }

  private DatasetId getDatasetId(String tableNamePrefix) {
    return NamespaceId.SYSTEM.dataset(tableNamePrefix + "v3");
  }
  @Override
  protected MetricsTable getTable(String name) throws Exception {
    // add v3 so that all the tests are performed for v3 table
    DatasetId metricsDatasetInstanceId = getDatasetId(name);
    DatasetProperties props = TableProperties.builder().setReadlessIncrementSupport(true).build();
    return DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId,
                                           MetricsTable.class.getName(), props, null);
  }
}
