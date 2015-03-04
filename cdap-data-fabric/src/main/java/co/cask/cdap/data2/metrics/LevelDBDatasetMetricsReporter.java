/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.proto.Id;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Collects LevelDB-based dataset's metrics from levelDB.
 */
// todo: consider extracting base class from HBaseDatasetMetricsReporter and LevelDBDatasetMetricsReporter
public class LevelDBDatasetMetricsReporter extends AbstractScheduledService implements DatasetMetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(LevelDBDatasetMetricsReporter.class);
  private static final int BYTES_IN_MB = 1024 * 1024;

  private final int reportIntervalInSec;

  private final MetricsCollectionService metricsService;
  private final LevelDBTableService ldbService;
  private final DatasetNamespace userDsNamespace;
  private final DatasetFramework dsFramework;

  private ScheduledExecutorService executor;

  @Inject
  public LevelDBDatasetMetricsReporter(MetricsCollectionService metricsService,
                                       LevelDBTableService ldbService, DatasetFramework dsFramework,
                                       CConfiguration conf) {
    this.metricsService = metricsService;
    this.ldbService = ldbService;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.LEVELDB_STATS_REPORT_INTERVAL);
    this.userDsNamespace = new DefaultDatasetNamespace(conf);
    this.dsFramework = dsFramework;
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
    Map<String, LevelDBTableService.TableStats> tableStats = ldbService.getTableStats();
    if (tableStats.size() > 0) {
      report(tableStats);
    }
  }

  private static boolean isValidDatasetId(String datasetId) {
    return CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9'))
      .or(CharMatcher.is('.'))
      .or(CharMatcher.is('$')).matchesAllOf(datasetId);
  }

  private void report(Map<String, LevelDBTableService.TableStats> datasetStat)
    throws DatasetManagementException {
    for (Map.Entry<String, LevelDBTableService.TableStats> statEntry : datasetStat.entrySet()) {
      Id.DatasetInstance datasetInstance = userDsNamespace.fromNamespaced(statEntry.getKey());
      if (datasetInstance == null) {
        // not a user dataset
        continue;
      }

      Collection<DatasetSpecification> instances = dsFramework.getInstances(datasetInstance.getNamespace());
      for (DatasetSpecification spec : instances) {
        if (statEntry.getKey().startsWith(spec.getName())) {
          datasetInstance = userDsNamespace.fromNamespaced(spec.getName());
          break;
        }
      }

      // use the first part of the dataset name, would use history if dataset name is history.objects.kv
      MetricsCollector collector =
        metricsService.getCollector(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE,
                                                    Constants.Metrics.Tag.DATASET, datasetInstance.getId()));

      metricsService.getCollector(ImmutableMap.of(Constants.Metrics.Tag.DATASET, datasetInstance.getId()));
      // legacy format: dataset name is in the tag. See DatasetInstantiator for more details
      int sizeInMb = (int) (statEntry.getValue().getDiskSizeBytes() / BYTES_IN_MB);
      collector.gauge("dataset.size.mb", 100);
    }
  }
}
