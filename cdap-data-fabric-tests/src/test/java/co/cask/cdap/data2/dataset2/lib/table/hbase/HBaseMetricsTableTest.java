/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.NamespaceClientUnitTestModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTableTest;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.process.MetricsTableMigration;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

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
  private static Configuration hConf;
  private static CConfiguration cConf;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    Injector injector = Guice.createInjector(new DataFabricModules().getDistributedModules(),
                                             new ConfigModule(cConf, TEST_HBASE.getConfiguration()),
                                             new ZKClientModule(),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new TransactionMetricsModule(),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new NamespaceClientUnitTestModule().getModule(),
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
    hConf = injector.getInstance(Configuration.class);
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

  @Test
  public void testCombinedScan() throws Exception {
    MetricsTable v2Table = getTable("V2Table");
    MetricsTable v3Table = getTable("V3Table");

    v2Table.put(ImmutableSortedMap.<byte[], SortedMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
                  .put(A, mapOf(A, Bytes.toLong(A), B, Bytes.toLong(B)))
                  .put(B, mapOf(A, Bytes.toLong(A), B, Bytes.toLong(B))).build());
    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(v2Table.get(A, A)));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v2Table.get(A, B)));
    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(v2Table.get(B, A)));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v2Table.get(B, B)));

    v3Table.put(ImmutableSortedMap.<byte[], SortedMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
                  .put(B, mapOf(B, Bytes.toLong(B), C, Bytes.toLong(C)))
                  .put(C, mapOf(P, Bytes.toLong(P), X, Bytes.toLong(X))).build());
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v3Table.get(B, B)));
    Assert.assertEquals(Bytes.toLong(C), Bytes.toLong(v3Table.get(B, C)));
    Assert.assertEquals(Bytes.toLong(X), Bytes.toLong(v3Table.get(C, X)));
    Assert.assertEquals(Bytes.toLong(P), Bytes.toLong(v3Table.get(C, P)));

    Scanner v2Scanner = v2Table.scan(null, null, null);
    Scanner v3Scanner = v3Table.scan(null, null, null);

    CombinedMetricsScanner combinedScanner = new CombinedMetricsScanner(v2Scanner, v3Scanner);

    Row firstRow = combinedScanner.next();
    Assert.assertEquals(1L, Bytes.toLong(firstRow.getRow()));
    Iterator<Map.Entry<byte[], byte[]>> colIterator = firstRow.getColumns().entrySet().iterator();
    Map.Entry<byte[], byte[]> column = colIterator.next();
    Assert.assertEquals(1L, Bytes.toLong(column.getKey()));
    Assert.assertEquals(1L, Bytes.toLong(column.getValue()));
    column = colIterator.next();
    Assert.assertEquals(2L, Bytes.toLong(column.getKey()));
    Assert.assertEquals(2L, Bytes.toLong(column.getValue()));

    Row secondRow = combinedScanner.next();
    Assert.assertEquals(2L, Bytes.toLong(secondRow.getRow()));
    colIterator = secondRow.getColumns().entrySet().iterator();
    column = colIterator.next();
    Assert.assertEquals(1L, Bytes.toLong(column.getKey()));
    Assert.assertEquals(1L, Bytes.toLong(column.getValue()));
    column = colIterator.next();
    Assert.assertEquals(2L, Bytes.toLong(column.getKey()));
    Assert.assertEquals(4L, Bytes.toLong(column.getValue()));
    column = colIterator.next();
    Assert.assertEquals(3L, Bytes.toLong(column.getKey()));
    Assert.assertEquals(3L, Bytes.toLong(column.getValue()));

    Row thirdRow = combinedScanner.next();
    Assert.assertEquals(3L, Bytes.toLong(thirdRow.getRow()));
    colIterator = thirdRow.getColumns().entrySet().iterator();
    column = colIterator.next();
    Assert.assertEquals(4L, Bytes.toLong(column.getKey()));
    Assert.assertEquals(4L, Bytes.toLong(column.getValue()));
    column = colIterator.next();
    Assert.assertEquals(7L, Bytes.toLong(column.getKey()));
    Assert.assertEquals(7L, Bytes.toLong(column.getValue()));
  }

  @Test
  public void testDataMigration() throws Exception {
    MetricsTable v2Table = getTable("V2Table");
    MetricsTable v3Table = getTable("V3Table");

    v2Table.put(ImmutableSortedMap.<byte[], SortedMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
                  .put(A, mapOf(A, Bytes.toLong(A), B, Bytes.toLong(B)))
                  .put(B, mapOf(A, Bytes.toLong(A), B, Bytes.toLong(B)))
                  .put(X, mapOf(A, Bytes.toLong(A), B, Bytes.toLong(B))).build());
    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(v2Table.get(A, A)));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v2Table.get(A, B)));
    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(v2Table.get(B, A)));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v2Table.get(B, B)));

    // add just column A value for key X in table v3, so this is an increment, while column B is a gauge.
    v3Table.put(ImmutableSortedMap.<byte[], SortedMap<byte[], Long>>orderedBy(Bytes.BYTES_COMPARATOR)
                  .put(X, mapOf(A, Bytes.toLong(A))).build());
    MetricsTableMigration metricsTableMigration = new MetricsTableMigration(v2Table, v3Table, cConf, hConf);
    Assert.assertTrue(metricsTableMigration.isOldMetricsDataAvailable());
    metricsTableMigration.transferData();

    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(v3Table.get(A, A)));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v3Table.get(A, B)));
    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(v3Table.get(B, A)));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v3Table.get(B, B)));

    // this is an increment
    Assert.assertEquals(Bytes.toLong(A) * 2, Bytes.toLong(v3Table.get(X, A)));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(v3Table.get(X, B)));

    Assert.assertFalse(metricsTableMigration.isOldMetricsDataAvailable());
  }

    @Override
  protected MetricsTable getTable(String name) throws Exception {
    DatasetId metricsDatasetInstanceId = NamespaceId.SYSTEM.dataset(name);
    DatasetProperties props = TableProperties.builder().setReadlessIncrementSupport(true)
      .add(Constants.Metrics.METICS_HBASE_TABLE_SPLITS, 16).build();
    return DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId,
                                           MetricsTable.class.getName(), props, null);
  }

}
