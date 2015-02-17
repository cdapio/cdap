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
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableService;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Collects LevelDB-based dataset's metrics from levelDB.
 */
// todo: consider extracting base class from HBaseDatasetMetricsReporter and LevelDBDatasetMetricsReporter
public class LevelDBDatasetMetricsReporter extends AbstractScheduledService implements DatasetMetricsReporter {
  private static final int BYTES_IN_MB = 1024 * 1024;

  private final int reportIntervalInSec;

  private final MetricsCollectionService metricsService;
  private final LevelDBOrderedTableService ldbService;
  private final DatasetNamespace userDsNamespace;

  private ScheduledExecutorService executor;

  @Inject
  public LevelDBDatasetMetricsReporter(MetricsCollectionService metricsService,
                                       LevelDBOrderedTableService ldbService,
                                       CConfiguration conf) {
    this.metricsService = metricsService;
    this.ldbService = ldbService;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.LEVELDB_STATS_REPORT_INTERVAL);
    this.userDsNamespace = new DefaultDatasetNamespace(conf, Namespace.USER);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    reportStats();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, reportIntervalInSec, TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("LevelDBDatasetMetricsReporter-scheduler"));
    return executor;
  }

  private void reportStats() throws Exception {
    Map<String, LevelDBOrderedTableService.TableStats> tableStats = ldbService.getTableStats();
    if (tableStats.size() > 0) {
      report(tableStats);
    }
  }

  private void report(Map<String, LevelDBOrderedTableService.TableStats> datasetStat) {
    for (Map.Entry<String, LevelDBOrderedTableService.TableStats> statEntry : datasetStat.entrySet()) {
      String datasetName = userDsNamespace.fromNamespaced(statEntry.getKey());
      if (datasetName == null) {
        // not a user dataset
        continue;
      }
      // use the first part of the dataset name, would use history if dataset name is history.objects.kv
      if (datasetName.contains(".")) {
        datasetName = datasetName.substring(0, datasetName.indexOf("."));
      }
      MetricsCollector collector =
        metricsService.getCollector(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE,
                                                    Constants.Metrics.Tag.DATASET, datasetName));
      // legacy format: dataset name is in the tag. See DatasetInstantiator for more details
      int sizeInMb = (int) (statEntry.getValue().getDiskSizeBytes() / BYTES_IN_MB);
      collector.gauge("dataset.size.mb", sizeInMb);
    }
  }
}
