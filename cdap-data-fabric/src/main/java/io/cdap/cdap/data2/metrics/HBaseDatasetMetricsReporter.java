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
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.common.Threads;

/**
 * Collects HBase-based dataset's metrics from HBase.
 */
// todo: consider extracting base class from HBaseDatasetMetricsReporter and LevelDBDatasetMetricsReporter
public class HBaseDatasetMetricsReporter extends AbstractScheduledService implements
    DatasetMetricsReporter {

  private final int reportIntervalInSec;
  private final MetricsCollectionService metricsService;
  private final Configuration hConf;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetFramework dsFramework;

  private ScheduledExecutorService executor;
  private HBaseAdmin hAdmin;


  @Inject
  public HBaseDatasetMetricsReporter(MetricsCollectionService metricsService,
      HBaseTableUtil hBaseTableUtil,
      Configuration hConf, CConfiguration conf, DatasetFramework dsFramework) {
    this.metricsService = metricsService;
    this.hBaseTableUtil = hBaseTableUtil;
    this.hConf = hConf;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.HBASE_STATS_REPORT_INTERVAL);
    this.dsFramework = dsFramework;
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

  private void reportHBaseStats()
      throws IOException, DatasetManagementException, UnauthorizedException {
    Map<TableId, HBaseTableUtil.TableStats> tableStats = hBaseTableUtil.getTableStats(hAdmin);
    if (tableStats.size() > 0) {
      report(tableStats);
    }
  }

  private void report(Map<TableId, HBaseTableUtil.TableStats> tableStats)
      throws IOException, UnauthorizedException {
    Map<String, String> reverseNamespaceMap = hBaseTableUtil.getHBaseToCDAPNamespaceMap();
    for (Map.Entry<TableId, HBaseTableUtil.TableStats> statEntry : tableStats.entrySet()) {
      String hbaseNamespace = statEntry.getKey().getNamespace();
      String cdapNamespace = reverseNamespaceMap.get(hbaseNamespace);
      // emit metrics for only user datasets, namespaces in system and
      // tableNames that doesn't start with user are ignored
      if (NamespaceId.SYSTEM.getNamespace().equals(cdapNamespace)) {
        continue;
      }
      String tableName = statEntry.getKey().getTableName();
      try {
        Collection<DatasetSpecificationSummary> instances = dsFramework.getInstances(
            new NamespaceId(cdapNamespace));
        for (DatasetSpecificationSummary spec : instances) {
          DatasetSpecification specification = dsFramework.getDatasetSpec(
              new DatasetId(cdapNamespace, spec.getName()));
          if (specification.isParent(tableName)) {
            MetricsContext collector =
                metricsService.getContext(
                    ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, cdapNamespace,
                        Constants.Metrics.Tag.DATASET, spec.getName()));
            collector.gauge("dataset.size.mb", statEntry.getValue().getTotalSizeMB());
            break;
          }
        }
      } catch (DatasetManagementException | ServiceUnavailableException e) {
        // No op
      }
    }
  }
}
