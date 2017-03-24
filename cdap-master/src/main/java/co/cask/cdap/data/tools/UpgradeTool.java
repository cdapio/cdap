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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.guice.TwillModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.service.Services;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.config.DefaultConfigStore;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.NoOpLineageWriter;
import co.cask.cdap.data2.registry.DefaultUsageRegistry;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.util.hbase.CoprocessorManager;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.internal.app.runtime.schedule.store.ScheduleStoreTableUtil;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.meta.LoggingStoreTableUtil;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.messaging.store.hbase.HBaseTableFactory;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.store.DefaultOwnerStore;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.distributed.TransactionService;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Command line tool for the Upgrade tool
 */
public class UpgradeTool {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeTool.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TransactionService txService;
  private final ZKClientService zkClientService;
  private final DatasetFramework dsFramework;
  private final DatasetUpgrader dsUpgrade;
  private final QueueAdmin queueAdmin;
  private final AuthorizationEnforcementService authorizationService;
  private final HBaseTableFactory tmsTableFactory;
  private final CoprocessorManager coprocessorManager;

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrades CDAP to " + ProjectInfo.getVersion() + "\n" +
              "  The upgrade tool upgrades the following: \n" +
              "  1. User and System Datasets (upgrades the coprocessor jars)\n" +
              "  2. Stream State Store\n" +
              "  Note: Once you run the upgrade tool you cannot rollback to the previous version."),
    UPGRADE_HBASE("After an HBase upgrade, updates the coprocessor jars of all user and \n" +
                    "system HBase tables to a version that is compatible with the new HBase \n" +
                    "version. All tables must be disabled prior to this step."),
    HELP("Show this help.");

    private final String description;

    Action(String description) {
      this.description = description;
    }

    private String getDescription() {
      return description;
    }
  }

  UpgradeTool() throws Exception {
    this.cConf = CConfiguration.create();
    if (this.cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      LOG.info("Disabling authorization for {}.", getClass().getSimpleName());
      this.cConf.setBoolean(Constants.Security.Authorization.ENABLED, false);
    }
    // Note: login has to happen before any objects that need Kerberos credentials are instantiated.
    SecurityUtil.loginForMasterService(cConf);

    this.hConf = HBaseConfiguration.create();
    Injector injector = createInjector();
    this.txService = injector.getInstance(TransactionService.class);
    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.dsFramework = injector.getInstance(DatasetFramework.class);
    this.dsUpgrade = injector.getInstance(DatasetUpgrader.class);
    this.queueAdmin = injector.getInstance(QueueAdmin.class);
    this.authorizationService = injector.getInstance(AuthorizationEnforcementService.class);
    this.tmsTableFactory = injector.getInstance(HBaseTableFactory.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    HBaseTableUtil tableUtil = injector.getInstance(HBaseTableUtil.class);
    this.coprocessorManager = new CoprocessorManager(cConf, locationFactory, tableUtil);


    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          UpgradeTool.this.stop();
        } catch (Throwable e) {
          LOG.error("Failed to upgrade", e);
        }
      }
    });
  }

  @VisibleForTesting
  Injector createInjector() throws Exception {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new MessagingClientModule(),
      Modules.override(new DataSetsModules().getDistributedModules()).with(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DatasetFramework.class).to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);
            // the DataSetsModules().getDistributedModules() binds to RemoteDatasetFramework so override that to
            // the same InMemoryDatasetFramework
            bind(DatasetFramework.class)
              .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
              .to(DatasetFramework.class);
            install(new FactoryModuleBuilder()
                      .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                      .build(DatasetDefinitionRegistryFactory.class));
            // CDAP-5954 Upgrade tool does not need to record lineage and metadata changes for now.
            bind(LineageWriter.class).to(NoOpLineageWriter.class);
          }
        }
      ),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new TwillModule(),
      new ExploreClientModule(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new ServiceStoreModules().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      // don't need real notifications for upgrade, so use the in-memory implementations
      new NotificationServiceRuntimeModule().getInMemoryModules(),
      new KafkaClientModule(),
      new NamespaceStoreModule().getDistributedModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new SecureStoreModules().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // the DataFabricDistributedModule needs MetricsCollectionService binding and since Upgrade tool does not do
          // anything with Metrics we just bind it to NoOpMetricsCollectionService
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          bind(MetricDatasetFactory.class).to(DefaultMetricDatasetFactory.class).in(Scopes.SINGLETON);
          bind(MetricStore.class).to(DefaultMetricStore.class);
        }

        @Provides
        @Singleton
        @Named("datasetInstanceManager")
        @SuppressWarnings("unused")
        public DatasetInstanceManager getDatasetInstanceManager(TransactionSystemClientService txClient,
                                                                TransactionExecutorFactory txExecutorFactory,
                                                                @Named("datasetMDS") DatasetFramework framework) {
          return new DatasetInstanceManager(txClient, txExecutorFactory, framework);
        }

        // This is needed because the LocalApplicationManager
        // expects a dsframework injection named datasetMDS
        @Provides
        @Singleton
        @Named("datasetMDS")
        @SuppressWarnings("unused")
        public DatasetFramework getInDsFramework(DatasetFramework dsFramework) {
          return dsFramework;
        }

      });
  }

  /**
   * Do the start up work
   * Note: includeNewDatasets boolean is required because upgrade tool has two mode: 1. Normal CDAP upgrade and
   * 2. Upgrading co processor for tables after hbase upgrade. This parameter specifies whether new system dataset
   * which were added in the current release needs to be added in the dataset framework or not.
   * During Normal CDAP upgrade (1) we don't need these datasets to be added in the ds framework as they will get
   * created during upgrade rather than when cdap starts after upgrade which is what we want.
   * Whereas during Hbase upgrade (2) we want these new tables to be added so that the co processor of these tables
   * can be upgraded when the user runs CDAP's Hbase Upgrade after upgrading to a newer version of Hbase.
   * @param includeNewDatasets boolean which specifies whether to add new datasets in ds framework or not
   */
  private void startUp(boolean includeNewDatasets) throws Exception {
    // Start all the services.
    LOG.info("Starting Zookeeper Client...");
    Services.startAndWait(zkClientService, cConf.getLong(Constants.Zookeeper.CLIENT_STARTUP_TIMEOUT_MILLIS),
                          TimeUnit.MILLISECONDS,
                          String.format("Connection timed out while trying to start ZooKeeper client. Please " +
                                          "verify that the ZooKeeper quorum settings are correct in cdap-site.xml. " +
                                          "Currently configured as: %s", cConf.get(Constants.Zookeeper.QUORUM)));
    LOG.info("Starting Transaction Service...");
    txService.startAndWait();
    authorizationService.startAndWait();
    LOG.info("Initializing Dataset Framework...");
    initializeDSFramework(cConf, dsFramework, includeNewDatasets);
    LOG.info("Building and uploading new HBase coprocessors...");
    coprocessorManager.ensureCoprocessorExists();
  }

  /**
   * Stop services and
   */
  private void stop() {
    try {
      txService.stopAndWait();
      zkClientService.stopAndWait();
      authorizationService.stopAndWait();
    } catch (Throwable e) {
      LOG.error("Exception while trying to stop upgrade process", e);
      Runtime.getRuntime().halt(1);
    }
  }

  private void doMain(String[] args) throws Exception {
    System.out.println(String.format("%s - version %s.", getClass().getSimpleName(), ProjectInfo.getVersion()));
    System.out.println();

    if (args.length < 1) {
      printHelp();
      return;
    }

    Action action = parseAction(args[0]);
    if (action == null) {
      System.out.println(String.format("Unsupported action : %s", args[0]));
      printHelp(true);
      return;
    }
    // Set to interactive mode to true by default
    boolean interactive = true;

    if ((args.length >= 2) && (args[1]).equals("force")) {
      interactive = false;
      System.out.println("Starting upgrade in non interactive mode.");
    }

    try {
      switch (action) {
        case UPGRADE: {
          System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
          String response = getResponse(interactive);
          if (response.equalsIgnoreCase("y") || response.equalsIgnoreCase("yes")) {
            System.out.println("Starting upgrade ...");
            try {
              startUp(false);
              performUpgrade();
              System.out.println("\nUpgrade completed successfully.\n");
            } finally {
              stop();
            }
          } else {
            System.out.println("Upgrade cancelled.");
          }
          break;
        }
        case UPGRADE_HBASE: {
          System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
          String response = getResponse(interactive);
          if (response.equalsIgnoreCase("y") || response.equalsIgnoreCase("yes")) {
            System.out.println("Starting upgrade ...");
            try {
              startUp(true);
              performHBaseUpgrade();
              System.out.println("\nUpgrade completed successfully.\n");
            } finally {
              stop();
            }
          } else {
            System.out.println("Upgrade cancelled.");
          }
          break;
        }
        case HELP:
          printHelp();
          break;
      }
    } catch (Exception e) {
      System.out.println(String.format("Failed to perform action '%s'. Reason: '%s'.", action, e.getMessage()));
      throw e;
    }
  }

  private String getResponse(boolean interactive) {
    if (interactive) {
      Scanner scan = new Scanner(System.in);
      System.out.println("Do you want to continue (y/n)");
      return scan.next();
    }
    return "y";
  }

  private void printHelp() {
    printHelp(false);
  }

  private void printHelp(boolean beginNewLine) {
    if (beginNewLine) {
      System.out.println();
    }
    System.out.println("Available actions: ");
    System.out.println();

    for (Action action : Action.values()) {
      System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
    }
  }

  private Action parseAction(String action) {
    try {
      return Action.valueOf(action.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private void performUpgrade() throws Exception {
    performCoprocessorUpgrade();
  }

  private void performHBaseUpgrade() throws Exception {
    System.setProperty(AbstractHBaseDataSetAdmin.SYSTEM_PROPERTY_FORCE_HBASE_UPGRADE, Boolean.TRUE.toString());
    performCoprocessorUpgrade();
  }

  private void performCoprocessorUpgrade() throws Exception {
    LOG.info("Disabling TMS Tables...");
    tmsTableFactory.disableMessageTable(cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME));
    tmsTableFactory.disablePayloadTable(cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME));

    LOG.info("Upgrading User and System HBase Tables ...");
    dsUpgrade.upgrade();

    LOG.info("Upgrading QueueAdmin ...");
    queueAdmin.upgrade();
  }

  public static void main(String[] args) {
    try {
      UpgradeTool upgradeTool = new UpgradeTool();
      upgradeTool.doMain(args);
    } catch (Throwable t) {
      LOG.error("Failed to upgrade ...", t);
      System.exit(1);
    }
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   * Note: includeNewDatasets boolean is required because upgrade tool has two mode: 1. Normal CDAP upgrade and
   * 2. Upgrading co processor for tables after hbase upgrade. This parameter specifies whether new system dataset
   * which were added in the current release needs to be added in the dataset framework or not.
   * During Normal CDAP upgrade (1) we don't need these datasets to be added in the ds framework as they will get
   * created during upgrade rather than when cdap starts after upgrade which is what we want.
   * Whereas during Hbase upgrade (2) we want these new tables to be added so that the co processor of these tables
   * can be upgraded when the user runs CDAP's Hbase Upgrade after upgrading to a newer version of Hbase.
   */
  private void initializeDSFramework(CConfiguration cConf,
                                     DatasetFramework datasetFramework, boolean includeNewDatasets)
    throws IOException, DatasetManagementException {
    // dataset service
    DatasetMetaTableUtil.setupDatasets(datasetFramework);
    // artifacts
    ArtifactStore.setupDatasets(datasetFramework);
    // Note: do no remove this if block even if it's empty. Read comment below and function doc above
    if (includeNewDatasets) {
      // Add all new system dataset introduced in the current release in this block. If no new dataset was introduced
      // then leave this block empty but do not remove block so that it can be used in next release if needed
      // owner meta
      DefaultOwnerStore.setupDatasets(datasetFramework);
    }
    // metadata and lineage
    DefaultMetadataStore.setupDatasets(datasetFramework);
    LineageStore.setupDatasets(datasetFramework);
    // app metadata
    DefaultStore.setupDatasets(datasetFramework);
    // config store
    DefaultConfigStore.setupDatasets(datasetFramework);
    // logs metadata
    LoggingStoreTableUtil.setupDatasets(datasetFramework);
    // scheduler metadata
    ScheduleStoreTableUtil.setupDatasets(datasetFramework);

    // metrics data
    DefaultMetricDatasetFactory factory = new DefaultMetricDatasetFactory(cConf, datasetFramework);
    DefaultMetricDatasetFactory.setupDatasets(factory);

    // Usage registry
    DefaultUsageRegistry.setupDatasets(datasetFramework);
  }
}
