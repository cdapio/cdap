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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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

/**
 * Test case for timeseries table cleanup.
 */
public class TimeSeriesCleanupTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static MetricsTableFactory tableFactory;

  @Test
  public void testDeleteBefore() throws OperationException {
    TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries("deleteTimeRange", 1);

    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // Insert metrics that spends multiple timbase
    insertMetrics(timeSeriesTable, "app.f.flow.flowlet", "runId", "metric",
                  ImmutableList.<String>of(), time, 0, 2000, 100);

    // Query metrics
    MetricsScanQuery query = new MetricsScanQueryBuilder()
      .setContext("app.f.flow.flowlet")
      .setMetric("metric")
      .build(time, time + 2000);

    MetricsScanner scanner = timeSeriesTable.scan(query);
    // Consume the scanner, check for number of rows scanned
    while (scanner.hasNext()) {
      scanner.next();
    }

    // 2000 timestamps, 300 per rows, hence 7 rows is expected
    Assert.assertEquals(7, scanner.getRowScanned());

    // Delete the first 600 timestamps (2 rows)
    timeSeriesTable.deleteBefore(time + 600);
    scanner = timeSeriesTable.scan(query);
    while (scanner.hasNext()) {
      scanner.next();
    }
    Assert.assertEquals(5, scanner.getRowScanned());

    // Delete before middle of the second row, one row is removed.
    timeSeriesTable.deleteBefore(time + 950);
    scanner = timeSeriesTable.scan(query);
    while (scanner.hasNext()) {
      scanner.next();
    }
    Assert.assertEquals(4, scanner.getRowScanned());

    // Delete except the last row.
    timeSeriesTable.deleteBefore(time + 1999);
    scanner = timeSeriesTable.scan(query);
    while (scanner.hasNext()) {
      MetricsScanResult result = scanner.next();
      for (TimeValue timeValue : result) {
        long ts = timeValue.getTime();
        Assert.assertTrue(ts >= time + 1800 && ts < time + 2000);
      }
    }
  }

  private void insertMetrics(TimeSeriesTable timeSeriesTable,
                             String context, String runId, String metric, Iterable<String> tags,
                             long startTime, int offset, int count, int batchSize) throws OperationException {

    List<TagMetric> tagMetrics = Lists.newLinkedList();
    List<MetricsRecord> records = Lists.newArrayListWithCapacity(batchSize);
    for (int i = offset; i < offset + count; i += batchSize) {
      for (int j = i; j < i + batchSize; j++) {
        for (String tag : tags) {
          tagMetrics.add(new TagMetric(tag, j * 2));
        }
        records.add(new MetricsRecord(context, runId, metric, tagMetrics, startTime + j, j, MetricType.COUNTER));
        tagMetrics.clear();
      }
      timeSeriesTable.save(records);
      records.clear();
    }
  }

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, "300");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricLevelDBModule(),
      new ConfigModule(cConf),
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
      new InMemoryDatasetFramework(injector.getInstance(DatasetDefinitionRegistryFactory.class));
    dsFramework.addModule("metrics-leveldb", new InMemoryMetricsTableModule());
    tableFactory = new DefaultMetricsTableFactory(cConf, dsFramework);
  }
}
