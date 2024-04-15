/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.data.tools;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles upgrade for System and User Datasets
 */
public class DatasetUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUpgrader.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final DatasetFramework dsFramework;
  private final Impersonator impersonator;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Pattern defaultNSUserTablePrefix;
  private final String datasetTablePrefix;


  @Inject
  DatasetUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
      NamespacePathLocator namespacePathLocator, DatasetFramework dsFramework,
      NamespaceQueryAdmin namespaceQueryAdmin, Impersonator impersonator) {
    super(locationFactory, namespacePathLocator);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.dsFramework = dsFramework;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.impersonator = impersonator;
    this.datasetTablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    this.defaultNSUserTablePrefix = Pattern.compile(
        String.format("^%s\\.user\\..*", datasetTablePrefix));
  }

  @Override
  public void upgrade() throws Exception {
    int numThreads = cConf.getInt(Constants.Upgrade.UPGRADE_THREAD_POOL_SIZE);
    ExecutorService executor =
        Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder()
                // all the threads should be created as 'cdap'
                .setThreadFactory(Executors.privilegedThreadFactory())
                .setNameFormat("dataset-upgrader-%d")
                .setDaemon(true)
                .build());
    try {
      // Upgrade system dataset
      upgradeSystemDatasets(executor);
    } finally {
      // We'll have tasks pending in the executor only on an interrupt, when user wants to abort the upgrade.
      // Use shutdownNow() to interrupt the tasks and abort.
      executor.shutdownNow();
    }
  }

  private void upgradeSystemDatasets(ExecutorService executor) throws Exception {
    Map<String, Future<?>> futures = new HashMap<>();
    for (final DatasetSpecificationSummary spec : dsFramework.getInstances(NamespaceId.SYSTEM)) {
      final DatasetId datasetId = NamespaceId.SYSTEM.dataset(spec.getName());
      Runnable runnable = new Runnable() {
        @Override
        public void run() {
          try {
            LOG.info("Upgrading dataset in system namespace: {}, spec: {}", spec.getName(),
                spec.toString());
            DatasetAdmin admin = dsFramework.getAdmin(datasetId, null);
            // we know admin is not null, since we are looping over existing datasets
            //noinspection ConstantConditions
            admin.upgrade();
            LOG.info("Upgraded dataset: {}", spec.getName());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

      Future<?> future = executor.submit(runnable);
      futures.put(datasetId.toString(), future);
    }

    // Wait for the system dataset upgrades to complete
    Map<String, Throwable> failed = waitForUpgrade(futures);
    if (!failed.isEmpty()) {
      for (Map.Entry<String, Throwable> entry : failed.entrySet()) {
        LOG.error("Failed to upgrade system dataset {}", entry.getKey(), entry.getValue());
      }
      throw new Exception(String.format("Error upgrading system datasets. %s of %s failed",
          failed.size(), futures.size()));
    }
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  private Map<String, Throwable> waitForUpgrade(Map<String, Future<?>> upgradeFutures)
      throws InterruptedException {
    Map<String, Throwable> failed = new HashMap<>();
    for (Map.Entry<String, Future<?>> entry : upgradeFutures.entrySet()) {
      try {
        entry.getValue().get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException && e.getCause().getCause() != null) {
          failed.put(entry.getKey(), e.getCause().getCause());
        } else {
          failed.put(entry.getKey(), e.getCause());
        }
      }
    }
    return failed;
  }
}
