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
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * migrates data from v2 table to v3 table with salting.
 */
public class DataMigrator implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DataMigrator.class);
  private static final List<Integer> MIGRATION_RESOLUTION_ORDER = Lists.newArrayList(Integer.MAX_VALUE, 3600, 60);

  private final LinkedHashMap<Integer, MetricsTableMigration> migrationOrder = new LinkedHashMap<>();

  // Data migrator thread will be created for each resolution tables, so they can be scheduled appropriately.
  public DataMigrator(MetricDatasetFactory metricDatasetFactory, CConfiguration cConf,
                      Configuration hConf) {
    for (int resolution : MIGRATION_RESOLUTION_ORDER) {
      migrationOrder.put(resolution, new MetricsTableMigration(metricDatasetFactory, resolution, cConf, hConf));
    }
  }

  @Override
  public void run() {
    // todo update it with looping over resolutions
    LOG.info("Starting Data Migration");
    MetricsTableMigration metricsTableMigration = migrationOrder.get(Integer.MAX_VALUE);
    if (metricsTableMigration.isOldMetricsDataAvailable()) {
      LOG.info("Metrics data is available in old table.. Running Migration");
      metricsTableMigration.transferData();
      LOG.info("Partial Transfer done");
    } else {

      LOG.info("Metrics data is not available in old table.. Skipping Migration");
    }
  }

}
