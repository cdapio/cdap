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

package io.cdap.cdap.data2.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.common.Threads;

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
  private static final int BYTES_IN_MB = 1024 * 1024;

  private final int reportIntervalInSec;
  private final MetricsCollectionService metricsService;
  private final LevelDBTableService ldbService;
  private final DatasetFramework dsFramework;
  private ScheduledExecutorService executor;

  @Inject
  public LevelDBDatasetMetricsReporter(MetricsCollectionService metricsService,
                                       LevelDBTableService ldbService, DatasetFramework dsFramework,
                                       CConfiguration conf) {
    this.metricsService = metricsService;
    this.ldbService = ldbService;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.LEVELDB_STATS_REPORT_INTERVAL);
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
    throws DatasetManagementException, UnauthorizedException {
    for (Map.Entry<TableId, LevelDBTableService.TableStats> statEntry : datasetStat.entrySet()) {
      String namespace = statEntry.getKey().getNamespace();
      // emit metrics for only user datasets, tables in system namespace are ignored
      if (NamespaceId.SYSTEM.getNamespace().equals(namespace)) {
        continue;
      }
      String tableName = statEntry.getKey().getTableName();

      Collection<DatasetSpecificationSummary> instances = dsFramework.getInstances(new NamespaceId(namespace));
      for (DatasetSpecificationSummary spec : instances) {
        DatasetSpecification specification = dsFramework.getDatasetSpec(new DatasetId(namespace, spec.getName()));
        if (specification.isParent(tableName)) {
          MetricsContext collector =
            metricsService.getContext(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, namespace,
                                                      Constants.Metrics.Tag.DATASET, spec.getName()));
          int sizeInMb = (int) (statEntry.getValue().getDiskSizeBytes() / BYTES_IN_MB);
          collector.gauge("dataset.size.mb", sizeInMb);
          break;
        }
      }
    }
  }

}
