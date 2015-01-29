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
package co.cask.cdap.metrics.data;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.metrics.transport.TagMetric;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;

/**
 *
 */
// todo: move/split/rename
public class LevelDBFilterableOVCTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static MetricsTableFactory tableFactory;
  private static final int rollTime = 60;

  @Test
  public void testAggregatesQuery() throws OperationException {
    AggregatesTable table = tableFactory.createAggregates("test");
    List<MetricsRecord> records = Lists.newLinkedList();
    List<TagMetric> tags = Lists.newArrayList();
    long ts = 1317470400;
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.reads", tags, ts, 10,
                                  MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.writes", tags, ts, 10,
                                  MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.attempts", tags, ts, 10,
                                  MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.errors", tags, ts, 10,
                                  MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "count.reads", tags, ts, 20,
                                  MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "count.writes", tags, ts, 20,
                                  MetricType.COUNTER));
    records.add(new MetricsRecord("app1.p.procedure1", "0", "count", tags, ts, 50,
                                  MetricType.COUNTER));
    table.update(records);

    // check that we get the right flow metrics for the app
    int flowlet1Count = 0;
    int flowlet2Count = 0;
    AggregatesScanner scanner = table.scan("app1.f", "count");
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      Assert.assertTrue(result.getContext().startsWith("app1.f"));
      if (result.getContext().equals("app1.f.flow1.flowlet1")) {
        Assert.assertEquals(10, result.getValue());
        flowlet1Count++;
      } else if (result.getContext().equals("app1.f.flow1.flowlet2")) {
        Assert.assertEquals(20, result.getValue());
        flowlet2Count++;
      }
    }
    Assert.assertEquals(4, flowlet1Count);
    Assert.assertEquals(2, flowlet2Count);

    // should only get 1 row back for this metric
    scanner = table.scan("app1.f.flow1", "count.errors");
    int rows = 0;
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      rows++;
      Assert.assertEquals("count.errors", result.getMetric());
      Assert.assertEquals("app1.f.flow1.flowlet1", result.getContext());
    }
    Assert.assertEquals(1, rows);
  }

  @Test
  public void testTimeseriesQuery() throws OperationException {
    TimeSeriesTable tsTable = tableFactory.createTimeSeries("test", 1);

    // one below the 1317470400 timebase
    long ts = 1317470399;

    List<MetricsRecord> records = Lists.newLinkedList();
    List<TagMetric> tags = Lists.newArrayList();
    Map<String, List<TimeValue>> expectedResults = Maps.newHashMap();

    int secondsToQuery = 3;

    String context = "app1.f.flow1.flowlet1";
    // insert time values that the query should not return
    for (int i = -2; i < secondsToQuery + 2; i++) {
      records.add(new MetricsRecord(context, "0", "reads", tags, ts + i, i, MetricType.COUNTER));
    }
    List<TimeValue> expectedFlowlet1Timevalues = Lists.newArrayListWithExpectedSize(secondsToQuery);
    for (int i = 0; i < secondsToQuery; i++) {
      expectedFlowlet1Timevalues.add(new TimeValue(ts + i, i));
    }
    expectedResults.put("app1.f.flow1.flowlet1", expectedFlowlet1Timevalues);

    // add some other metrics in other contexts that should not get returned
    records.add(new MetricsRecord("app1.p.procedure1", "0", "reads", tags, ts, 5, MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow2.flowlet1", "0", "reads", tags, ts, 10, MetricType.COUNTER));

    // this one should get returned
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "reads", tags, ts, 15, MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "reads", tags, ts + secondsToQuery - 1, 20,
                                  MetricType.COUNTER));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "reads", tags, ts + secondsToQuery + 5, 100,
                                  MetricType.COUNTER));
    expectedResults.put("app1.f.flow1.flowlet2",
                        Lists.newArrayList(new TimeValue(ts, 15), new TimeValue(ts + secondsToQuery - 1, 20)));
    tsTable.save(records);

    MetricsScanQuery query = new MetricsScanQueryBuilder()
      .setContext("app1.f.flow1")
      .setMetric("reads")
      .build(ts, ts + secondsToQuery - 1);
    MetricsScanner scanner = tsTable.scan(query);

    Map<String, List<TimeValue>> actualResults = Maps.newHashMap();
    while (scanner.hasNext()) {
      MetricsScanResult result = scanner.next();

      // check the metric
      Assert.assertTrue(result.getMetric().startsWith("reads"));

      List<TimeValue> metricTimeValues = actualResults.get(result.getContext());
      if (metricTimeValues == null) {
        metricTimeValues = Lists.newArrayList();
        actualResults.put(result.getContext(), metricTimeValues);
      }
      for (TimeValue tv : result) {
        metricTimeValues.add(tv);
      }
    }
    assertEqualResults(expectedResults, actualResults);
  }

  private void assertEqualResults(Map<String, List<TimeValue>> expected, Map<String, List<TimeValue>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (String context : expected.keySet()) {
      List<TimeValue> expectedValues = expected.get(context);
      List<TimeValue> actualValues = actual.get(context);
      Assert.assertEquals(expectedValues.size(), actualValues.size());

      // check all the values seen are expected
      for (TimeValue actualVal : actualValues) {
        TimeValue toRemove = null;
        for (TimeValue expectedVal : expectedValues) {
          if ((expectedVal.getTime() == actualVal.getTime()) && (expectedVal.getValue() == actualVal.getValue())) {
            toRemove = expectedVal;
            break;
          }
        }
        Assert.assertNotNull(toRemove);
        expectedValues.remove(toRemove);
      }
    }
  }


  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, String.valueOf(rollTime));
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DataFabricLevelDBModule(),
      new LocationRuntimeModule().getStandaloneModules(),
      new TransactionMetricsModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class,
                               DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
        }
      });
    DatasetFramework dsFramework =
      new InMemoryDatasetFramework(injector.getInstance(DatasetDefinitionRegistryFactory.class),
                                   injector.getInstance(CConfiguration.class));
    dsFramework.addModule("metrics-leveldb", new InMemoryMetricsTableModule());
    tableFactory = new DefaultMetricsTableFactory(cConf, dsFramework);
  }
}
