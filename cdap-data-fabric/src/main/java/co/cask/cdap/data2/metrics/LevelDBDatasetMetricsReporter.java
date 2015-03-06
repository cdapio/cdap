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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
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
  private final CConfiguration conf;
  private final DatasetFramework dsFramework;

  private ScheduledExecutorService executor;

  @Inject
  public LevelDBDatasetMetricsReporter(MetricsCollectionService metricsService,
                                       LevelDBTableService ldbService, DatasetFramework dsFramework,
                                       CConfiguration conf) {
    this.metricsService = metricsService;
    this.ldbService = ldbService;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.LEVELDB_STATS_REPORT_INTERVAL);
    this.conf = conf;
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
    Map<TableId, LevelDBTableService.TableStats> tableStats = ldbService.getTableStats();
    if (tableStats.size() > 0) {
      report(tableStats);
    }
  }

  private void report(Map<TableId, LevelDBTableService.TableStats> datasetStat)
    throws DatasetManagementException {
    for (Map.Entry<TableId, LevelDBTableService.TableStats> statEntry : datasetStat.entrySet()) {
      String namespace = statEntry.getKey().getNamespace().getId();
      // emit metrics for only user datasets, tables in system namespace are ignored
      if (namespace.equals("system")) {
        continue;
      }
      String tableName = statEntry.getKey().getTableName();

      Collection<DatasetSpecificationSummary> instances = dsFramework.getInstances(Id.Namespace.from(namespace));
      for (DatasetSpecificationSummary spec : instances) {
        // todo :  we are stripping cdap.{namespace} right now , this can be removed after namespace fixes
        // and logic can be moved to DatasetSpecification
        String dsName = stripRootPrefixAndNamespace(spec.getName());
        if (tableName.startsWith(dsName)) {
          // use the first part of the dataset name, would use history if dataset name is history.objects.kv
          MetricsCollector collector =
            metricsService.getCollector(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, namespace,
                                                        Constants.Metrics.Tag.DATASET, dsName));


          int sizeInMb = (int) (statEntry.getValue().getDiskSizeBytes() / BYTES_IN_MB);
          collector.gauge("dataset.size.mb", sizeInMb);
          break;
        }
      }

    }
  }

  private String stripRootPrefixAndNamespace(String dsName) {
    // ignoring "cdap." and namespace at beginning
    String rootPrefix = conf.get(Constants.Dataset.TABLE_PREFIX) + ".";
    if (dsName.startsWith(rootPrefix)) {
      dsName = dsName.substring(rootPrefix.length());
      // remove namespace part
      dsName = dsName.substring(dsName.indexOf(".") + 1);
    }
    return dsName;
  }

}
