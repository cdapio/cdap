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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.ImpersonationUtils;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * Handles upgrade for System and User Datasets
 */
public class DatasetUpgrader extends AbstractUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUpgrader.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final HBaseTableUtil hBaseTableUtil;
  private final DatasetFramework dsFramework;
  private final Impersonator impersonator;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Pattern defaultNSUserTablePrefix;
  private final String datasetTablePrefix;
  private final HBaseDDLExecutorFactory ddlExecutorFactory;


  @Inject
  DatasetUpgrader(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                  NamespacedLocationFactory namespacedLocationFactory,
                  HBaseTableUtil hBaseTableUtil, DatasetFramework dsFramework,
                  NamespaceQueryAdmin namespaceQueryAdmin, Impersonator impersonator) {
    super(locationFactory, namespacedLocationFactory);
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.hBaseTableUtil = hBaseTableUtil;
    this.dsFramework = dsFramework;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.impersonator = impersonator;
    this.datasetTablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    this.defaultNSUserTablePrefix = Pattern.compile(String.format("^%s\\.user\\..*", datasetTablePrefix));
    this.ddlExecutorFactory = new HBaseDDLExecutorFactory(cConf, hConf);
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

      // Upgrade all user hbase tables
      upgradeUserTables(executor);
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
        public void run() {
          try {
            LOG.info("Upgrading dataset in system namespace: {}, spec: {}", spec.getName(), spec.toString());
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

  private void upgradeUserTables(final ExecutorService executor) throws Exception {
    final Map<String, Future<?>> allFutures = new HashMap<>();
    final List<Closeable> toClose = new ArrayList<>();
    for (final NamespaceMeta namespaceMeta : namespaceQueryAdmin.list()) {
      impersonator.doAs(namespaceMeta.getNamespaceId(), new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          HBaseDDLExecutor ddlExecutor = ddlExecutorFactory.get();
          // register it for close, after all Futures are complete
          toClose.add(ddlExecutor);
          Map<String, Future<?>> futures = upgradeUserTables(namespaceMeta, executor, ddlExecutor);
          allFutures.putAll(futures);
          return null;
        }
      });
    }

    // Wait for the user dataset upgrades to complete
    try {
      Map<String, Throwable> failed = waitForUpgrade(allFutures);
      if (!failed.isEmpty()) {
        for (Map.Entry<String, Throwable> entry : failed.entrySet()) {
          LOG.error("Failed to upgrade user table {}", entry.getKey(), entry.getValue());
        }
        throw new Exception(String.format("Error upgrading user tables. %s of %s failed",
                                          failed.size(), allFutures.size()));
      }
    } finally {
      for (Closeable closeable : toClose) {
        Closeables.closeQuietly(closeable);
      }
    }
  }

  private Map<String, Future<?>> upgradeUserTables(final NamespaceMeta namespaceMeta, final ExecutorService executor,
                                                   final HBaseDDLExecutor ddlExecutor) throws Exception {
    Map<String, Future<?>> futures = new HashMap<>();
    String hBaseNamespace = hBaseTableUtil.getHBaseNamespace(namespaceMeta);
    try (HBaseAdmin hAdmin = new HBaseAdmin(hConf)) {
      for (final HTableDescriptor desc :
        hAdmin.listTableDescriptorsByNamespace(HTableNameConverter.encodeHBaseEntity(hBaseNamespace))) {
        Callable<Void> callable = new Callable<Void>() {
          public Void call() throws Exception {
            if (isCDAPUserTable(desc)) {
              upgradeUserTable(desc);
            } else if (isStreamOrQueueTable(desc.getNameAsString())) {
              updateTableDesc(desc, ddlExecutor);
            }
            return null;
          }
        };

        Future<?> future =
          executor.submit(ImpersonationUtils.createImpersonatingCallable(impersonator, namespaceMeta, callable));
        futures.put(desc.getNameAsString(), future);
      }
    }
    return futures;
  }

  private void upgradeUserTable(HTableDescriptor desc) throws IOException {
    TableId tableId = HTableNameConverter.from(desc);
    LOG.info("Upgrading hbase table: {}, desc: {}", tableId, desc);

    final boolean supportsIncrement = HBaseTableAdmin.supportsReadlessIncrements(desc);
    final boolean transactional = HBaseTableAdmin.isTransactional(desc);
    DatasetAdmin admin = new AbstractHBaseDataSetAdmin(tableId, hConf, cConf, hBaseTableUtil, locationFactory) {
      @Override
      protected CoprocessorJar createCoprocessorJar() throws IOException {
        return HBaseTableAdmin.createCoprocessorJarInternal(cConf,
                                                            coprocessorManager,
                                                            hBaseTableUtil,
                                                            transactional,
                                                            supportsIncrement);
      }

      @Override
      protected boolean needsUpdate(HTableDescriptor tableDescriptor, HTableDescriptorBuilder descriptorBuilder) {
        return false;
      }

      @Override
      public void create() throws IOException {
        // no-op
        throw new UnsupportedOperationException("This DatasetAdmin is only used for upgrade() operation");
      }
    };
    admin.upgrade();
    LOG.info("Upgraded hbase table: {}", tableId);
  }

  private void updateTableDesc(HTableDescriptor desc, HBaseDDLExecutor ddlExecutor) throws IOException {
    hBaseTableUtil.setVersion(desc);
    hBaseTableUtil.setHBaseVersion(desc);
    hBaseTableUtil.setTablePrefix(desc);
    hBaseTableUtil.modifyTable(ddlExecutor, desc);
  }

  private boolean isCDAPUserTable(HTableDescriptor desc) {
    String tableName = desc.getNameAsString();
    // If table is in system namespace: (starts with <tablePrefix>_system
    // or if it is not created by CDAP it is not user table
    if (tableName.startsWith(String.format("%s_%s", this.datasetTablePrefix, NamespaceId.SYSTEM.getEntityName())) ||
       (!isTableCreatedByCDAP(desc))) {
      return false;
    }
    // User tables are named differently in default vs non-default namespace
    // User table in default namespace starts with cdap.user
    // User table in Non-default namespace is a table that doesn't have
    //    system.queue or system.stream or system.sharded.queue
    return defaultNSUserTablePrefix.matcher(tableName).matches() ||
      // Note: if the user has created a dataset called system.* then we will not upgrade the table.
      // CDAP-2977 should be fixed to have a cleaner fix for this.
      !(isStreamOrQueueTable(tableName));
  }

  private boolean isStreamOrQueueTable(String tableName) {
    // table name should start with "cdap_" or "cdap." for versions 3.4 or earlier (before namespace mapping)
    return tableName.startsWith(datasetTablePrefix) && (tableName.contains("system.queue") ||
      tableName.contains("system.stream") || tableName.contains("system.sharded.queue"));
  }

  // Note: This check can be safely used for user table since we create meta.
  // CDAP-2963 should be fixed so that we can make use of this check generically for all cdap tables
  private boolean isTableCreatedByCDAP(HTableDescriptor desc) {
    return (desc.getValue(HBaseTableUtil.CDAP_VERSION) != null);
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  private Map<String, Throwable> waitForUpgrade(Map<String, Future<?>> upgradeFutures) throws InterruptedException {
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
