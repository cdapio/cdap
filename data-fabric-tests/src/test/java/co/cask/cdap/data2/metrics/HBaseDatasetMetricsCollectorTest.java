/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.metrics;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTableAdmin;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseOrderedTableDefinition;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.inmemory.DetachedTxSystemClient;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HBaseDatasetMetricsCollectorTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static CConfiguration conf = CConfiguration.create();
  private static HBaseTestBase testHBase;
  private static HBaseTableUtil hBaseTableUtil = new HBaseTableUtilFactory().get();
  private static DatasetNamespace userDsNamespace = new DefaultDatasetNamespace(conf, Namespace.USER);

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase(false);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testHBase.stopHBase();
  }

  @Test
  public void testHBaseTableDatasetMetrics() throws Exception {
    // no table -> no metrics
    Assert.assertEquals(0, getDatasetMetrics().size());

    // create two tables, check that they appear in metrics
    String table1Name = userDsNamespace.namespace("table1");
    String table2Name = userDsNamespace.namespace("table2");
    getTableAdmin(table1Name).create();
    getTableAdmin(table2Name).create();

    // todo: figure out how to make metrics update more frequently. Tried a lot, but still no luck
    TimeUnit.SECONDS.sleep(65);

    Map<String, Map<String, Integer>> metrics = getDatasetMetrics();
    // NOTE: currently in metrics, dataset names are not namespaced
    // remembering size of the table for later comparisons. NOTE: even though it is empty HBase can tell >0 size
    Integer table1Size = metrics.get("dataset.store.bytes").get("table1");
    Assert.assertNotNull(table1Size);
    Integer table2Size = metrics.get("dataset.store.bytes").get("table2");
    Assert.assertNotNull(table2Size);

    // write smth to one of the tables, check that its size > 0, while the size of the other one remains same
    HBaseOrderedTable table1 = getTable(table1Name);

    Transaction tx = new DetachedTxSystemClient().startShort();
    table1.startTx(tx);
    for (int i = 0; i < 1000; i++) {
      table1.put(Bytes.toBytes("row" + i), Bytes.toBytes("col" + i), Bytes.toBytes("val" + i));
    }
    table1.commitTx();

    TimeUnit.SECONDS.sleep(65);

    metrics = getDatasetMetrics();
    Integer newTable1Size = metrics.get("dataset.store.bytes").get("table1");
    Assert.assertTrue(newTable1Size > table1Size);
    table1Size = newTable1Size;
    Assert.assertEquals(table2Size, metrics.get("dataset.store.bytes").get("table2"));

    // Truncate table1, size metric should decrease
    getTableAdmin(table1Name).truncate();
    getTableAdmin(table2Name).truncate();

    TimeUnit.SECONDS.sleep(65);

    metrics = getDatasetMetrics();
    newTable1Size = metrics.get("dataset.store.bytes").get("table1");
    Assert.assertTrue(newTable1Size < table1Size);
    Assert.assertEquals(table2Size, metrics.get("dataset.store.bytes").get("table2"));

    // Drop tables, metric should disappear
    getTableAdmin(table1Name).drop();
    getTableAdmin(table2Name).drop();

    TimeUnit.SECONDS.sleep(65);

    Assert.assertEquals(0, getDatasetMetrics().size());
  }

  private static HBaseOrderedTable getTable(String name) throws Exception {
    // "true" for enableReadlessIncrements
    return new HBaseOrderedTable(name, ConflictDetection.ROW, testHBase.getConfiguration(), true);
  }

  private static HBaseOrderedTableAdmin getTableAdmin(String name) throws IOException {
    DatasetSpecification spec = new HBaseOrderedTableDefinition("foo").configure(name, DatasetProperties.EMPTY);
    return new HBaseOrderedTableAdmin(spec, testHBase.getConfiguration(), hBaseTableUtil,
                                      CConfiguration.create(), new LocalLocationFactory(tmpFolder.newFolder()));
  }

  private static Map<String, Map<String, Integer>> getDatasetMetrics() throws IOException {
    TableMetricsCollection metrics = new TableMetricsCollection();
    HBaseDatasetMetricsCollector.collect(metrics, testHBase.getConfiguration(), conf);
    return metrics.getMetrics(MetricsScope.REACTOR, Constants.Metrics.DATASET_CONTEXT);
  }

  private static final class TableMetricsCollection extends NoOpMetricsCollectionService {
    // scope -> context -> metric name -> tag -> value
    private final Map<MetricsScope, Map<String, Map<String, Map<String, Integer>>>> metrics = Maps.newHashMap();

    public Map<String, Map<String, Integer>> getMetrics(MetricsScope scope, final String context) {
      if (!metrics.containsKey(scope)) {
        return Maps.newHashMap();
      }
      if (!metrics.get(scope).containsKey(context)) {
        return Maps.newHashMap();
      }

      return metrics.get(scope).get(context);
    }

    @Override
    public MetricsCollector getCollector(final MetricsScope scope, final String context, final String runIdIgnored) {
      if (!metrics.containsKey(scope)) {
        metrics.put(scope, new HashMap<String, Map<String, Map<String, Integer>>>());
      }
      if (!metrics.get(scope).containsKey(context)) {
        metrics.get(scope).put(context, new HashMap<String, Map<String, Integer>>());
      }
      return new MetricsCollector() {
        @Override
        public void gauge(String metricName, int value, String... tags) {
          if (!metrics.get(scope).get(context).containsKey(metricName)) {
            metrics.get(scope).get(context).put(metricName, new HashMap<String, Integer>());
          }
          for (String tag : tags) {
            metrics.get(scope).get(context).get(metricName).put(tag, value);
          }
        }
      };
    }
  }
}
