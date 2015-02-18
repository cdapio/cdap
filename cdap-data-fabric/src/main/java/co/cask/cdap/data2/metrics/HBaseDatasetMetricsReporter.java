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

package co.cask.cdap.data2.metrics;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
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
// todo: consider extracting base class from HBaseDatasetMetricsReporter and LevelDBDatasetMetricsReporter
public class HBaseDatasetMetricsReporter extends AbstractScheduledService implements DatasetMetricsReporter {
  private final int reportIntervalInSec;

  private final MetricsCollectionService metricsService;
  private final Configuration hConf;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetNamespace userDsNamespace;

  private ScheduledExecutorService executor;
  private HBaseAdmin hAdmin;

  @Inject
  public HBaseDatasetMetricsReporter(MetricsCollectionService metricsService, HBaseTableUtil hBaseTableUtil,
                                     Configuration hConf, CConfiguration conf) {
    this.metricsService = metricsService;
    this.hBaseTableUtil = hBaseTableUtil;
    this.hConf = hConf;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.HBASE_STATS_REPORT_INTERVAL);
    this.userDsNamespace = new DefaultDatasetNamespace(conf, Namespace.USER);
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
      Threads.createDaemonThreadFactory("HBaseDatasetMetricsReporter-scheduler"));
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
    for (Map.Entry<String, HBaseTableUtil.TableStats> statEntry : datasetStat.entrySet()) {
      String datasetName = userDsNamespace.fromNamespaced(statEntry.getKey());
      if (datasetName == null) {
        // not a user dataset
        continue;
      }
      MetricsCollector collector =
        metricsService.getCollector(ImmutableMap.of(MetricTags.DATASET.getCodeName(), datasetName));
      // legacy format: dataset name is in the tag. See DatasetInstantiator for more details
      collector.increment("dataset.size.mb", statEntry.getValue().getTotalSizeMB());
    }
  }
}
