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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.common.Threads;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Collects HBase-based dataset's metrics from HBase.
 */
public class HBaseDatasetStatsReporter extends AbstractScheduledService {
  private volatile ScheduledExecutorService executor;
  private final int reportIntervalInSec;

  private final MetricsCollectionService metricsService;
  private final Configuration hConf;
  private final HBaseTableUtil hBaseTableUtil;

  private HBaseAdmin hAdmin;

  public HBaseDatasetStatsReporter(MetricsCollectionService metricsService, HBaseTableUtil hBaseTableUtil,
                                   Configuration hConf, CConfiguration conf) {
    this.metricsService = metricsService;
    this.hBaseTableUtil = hBaseTableUtil;
    this.hConf = hConf;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.HBASE_STATS_REPORT_INTERVAL);
  }

  @Override
  protected void startUp() throws Exception {
    hAdmin = new HBaseAdmin(hConf);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
    if (hAdmin != null) {
      hAdmin.close();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    reportHBaseStats();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, reportIntervalInSec, TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("HBaseDatasetStatsReporter-scheduler"));
    return executor;
  }

  private void reportHBaseStats() throws IOException {
    Map<String, HBaseTableUtil.TableStats> tableStats = hBaseTableUtil.getTableStats(hAdmin);
    if (tableStats.size() > 0) {
      report(tableStats);
    }
  }

  private void report(Map<String, HBaseTableUtil.TableStats> datasetStat) {
    // we use "0" as runId: it is required by metrics system to provide something at this point
    MetricsCollector collector =
      metricsService.getCollector(MetricsScope.REACTOR, Constants.Metrics.DATASET_CONTEXT, "0");
    for (Map.Entry<String, HBaseTableUtil.TableStats> statEntry : datasetStat.entrySet()) {
      // table name = dataset name for metrics
      String datasetName = statEntry.getKey();
      // legacy format: dataset name is in the tag. See DatasetInstantiator for more details
      collector.gauge("dataset.store.bytes", statEntry.getValue().getTotalSizeMB(), datasetName);
    }
  }
}
