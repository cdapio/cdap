/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
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
        records.add(new MetricsRecord(context, runId, metric, tagMetrics, startTime + j, j));
        tagMetrics.clear();
      }
      timeSeriesTable.save(records);
      records.clear();
    }
  }

  @BeforeClass
  public static void init() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, "300");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricLevelDBModule(),
      new ConfigModule(cConf),
      new TransactionMetricsModule(),
      new PrivateModule() {

        @Override
        protected void configure() {
          try {
            bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
            expose(MetricsTableFactory.class);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });
    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }
}
