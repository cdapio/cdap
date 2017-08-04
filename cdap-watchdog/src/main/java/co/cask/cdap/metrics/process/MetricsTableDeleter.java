/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.cdap.metrics.process;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deletes old v2 metrics tables when they no longer have any data. ie - data migration to new v3 table is complete
 */
public class MetricsTableDeleter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableDeleter.class);

  private final Map<Integer, MetricsTableMigration> resolutionTablesToDelete;
  private final HBaseTableUtil hBaseTableUtil;
  private final HBaseDDLExecutorFactory hBaseDDLExecutorFactory;


  public MetricsTableDeleter(MetricDatasetFactory metricDatasetFactory, CConfiguration cConf,
                             Configuration hConf,
                             HBaseTableUtil hBaseTableUtil, List<Integer> resolutions) {
    resolutionTablesToDelete = new HashMap<>();
    for (int resolution : resolutions) {
      resolutionTablesToDelete.put(resolution,
                                   new MetricsTableMigration(metricDatasetFactory, resolution, cConf, hConf));
    }
    this.hBaseTableUtil = hBaseTableUtil;
    this.hBaseDDLExecutorFactory = new HBaseDDLExecutorFactory(cConf, hConf);
  }

  @Override
  public void run() {
    for (int resolution : resolutionTablesToDelete.keySet()) {
      MetricsTableMigration metricsTableMigration = resolutionTablesToDelete.get(resolution);
      if (metricsTableMigration.v2MetricsTableExists(hBaseTableUtil, resolution)) {
        if (metricsTableMigration.deleteV2MetricsTable(hBaseDDLExecutorFactory, hBaseTableUtil, resolution)) {
          LOG.info("Successfully Deleted the v2 metrics table for resolution {} seconds", resolution);
        }
      }
    }
  }
}
