/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.TwillModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.config.DefaultConfigStore;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.runtime.schedule.store.ScheduleStoreTableUtil;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.distributed.TransactionService;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * Command line tool for the Upgrade tool
 */
public class UpgradeTool {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeTool.class);

  @VisibleForTesting
  final Injector injector;

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TransactionService txService;
  private final ZKClientService zkClientService;
  private final MDSDatasetsRegistry mdsDatasetsRegistry;

  private final DatasetFramework dsFramework;

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrades CDAP to 3.0\n" +
              "  The upgrade tool upgrades the following: \n" +
              "  1. User Datasets\n" +
              "      - Upgrades the coprocessor jars for tables\n" +
              "      - Migrates the metadata for PartitionedFileSets\n" +
              "  2. System Datasets\n" +
              "  3. UsageRegistry Dataset Type\n" +
              "  Note: Once you run the upgrade tool you cannot rollback to the previous version."),
    HELP("Show this help.");

    private final String description;

    Action(String description) {
      this.description = description;
    }

    private String getDescription() {
      return description;
    }
  }

  public UpgradeTool() throws Exception {
    this.cConf = CConfiguration.create();
    this.hConf = HBaseConfiguration.create();
    this.injector = init();
    this.txService = injector.getInstance(TransactionService.class);
    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.dsFramework = injector.getInstance(DatasetFramework.class);
    this.mdsDatasetsRegistry = injector.getInstance(Key.get(MDSDatasetsRegistry.class,
                                                            Names.named("mdsDatasetsRegistry")));

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

  private Injector init() throws Exception {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new TwillModule(),
      new ExploreClientModule(),
      Modules.override(new DataSetsModules().getDistributedModules()).with(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DatasetFramework.class).to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);
          }
        }
      ),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new ServiceStoreModules().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new KafkaClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new DataFabricDistributedModule());
          // the DataFabricDistributedModule needs MetricsCollectionService binding and since Upgrade tool does not do
          // anything with Metrics we just bind it to NoOpMetricsCollectionService
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          bind(MetricDatasetFactory.class).to(DefaultMetricDatasetFactory.class).in(Scopes.SINGLETON);
          bind(MetricStore.class).to(DefaultMetricStore.class);
        }
        
        @Provides
        @Singleton
        @Named("mdsDatasetsRegistry")
        public MDSDatasetsRegistry getMDSDatasetsRegistry(TransactionSystemClient txClient,
                                                          @Named("datasetMDS") DatasetFramework framework) {
          return new MDSDatasetsRegistry(txClient, framework);
        }

        @Provides
        @Singleton
        @Named("datasetInstanceManager")
        public DatasetInstanceManager getDatasetInstanceManager(@Named("mdsDatasetsRegistry")
                                                                MDSDatasetsRegistry mdsDatasetsRegistry) {
          return new DatasetInstanceManager(mdsDatasetsRegistry);
        }

        // This is needed because the LocalAdapterManager, LocalApplicationManager, LocalApplicationTemplateManager
        // expects a dsframework injection named datasetMDS
        @Provides
        @Singleton
        @Named("datasetMDS")
        public DatasetFramework getInDsFramework(DatasetFramework dsFramework) {
          return dsFramework;
        }

      });
  }

  /**
   * Do the start up work
   */
  private void startUp() throws Exception {
    // Start all the services.
    zkClientService.startAndWait();
    txService.startAndWait();
    initializeDSFramework(cConf, dsFramework);
    mdsDatasetsRegistry.startUp();
  }

  /**
   * Stop services and
   */
  private void stop() {
    try {
      txService.stopAndWait();
      zkClientService.stopAndWait();
      mdsDatasetsRegistry.shutDown();
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
        case UPGRADE:
          System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
          String response = getResponse(interactive);
          if (response.equalsIgnoreCase("y") || response.equalsIgnoreCase("yes")) {
            System.out.println("Starting upgrade ...");
            try {
              startUp();
              performUpgrade();
            } finally {
              stop();
            }
          } else {
            System.out.println("Upgrade cancelled.");
          }
          break;
        case HELP:
          printHelp();
          break;
      }
    } catch (Exception e) {
      System.out.println(String.format("Failed to perform action '%s'. Reason: '%s'.", action, e.getMessage()));
      e.printStackTrace(System.out);
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
    LOG.info("Upgrading System and User Datasets ...");
    DatasetUpgrader dsUpgrade = injector.getInstance(DatasetUpgrader.class);
    dsUpgrade.upgrade();

    LOG.info("Upgrading PartitionedFileSets ...");
    PFSUpgrader pfsUpgrader = injector.getInstance(PFSUpgrader.class);
    pfsUpgrader.upgrade();

    LOG.info("Upgrading QueueAdmin ...");
    QueueAdmin queueAdmin = injector.getInstance(QueueAdmin.class);
    queueAdmin.upgrade();

    UsageRegistry usageRegistry = injector.getInstance(UsageRegistry.class);
    usageRegistry.upgrade(injector.getInstance(Key.get(DatasetInstanceManager.class,
                                                       Names.named("datasetInstanceManager"))));
  }

  public static void main(String[] args) {
    try {
      UpgradeTool upgradeTool = new UpgradeTool();
      upgradeTool.doMain(args);
    } catch (Throwable t) {
      LOG.error("Failed to upgrade ...", t);
    }
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   */
  private void initializeDSFramework(CConfiguration cConf, DatasetFramework datasetFramework) throws IOException,
    DatasetManagementException {
    // dataset service
    DatasetMetaTableUtil.setupDatasets(datasetFramework);
    // app metadata
    DefaultStore.setupDatasets(datasetFramework);
    // config store
    DefaultConfigStore.setupDatasets(datasetFramework);
    // logs metadata
    LogSaverTableUtil.setupDatasets(datasetFramework);
    // scheduler metadata
    ScheduleStoreTableUtil.setupDatasets(datasetFramework);

    // metrics data
    DefaultMetricDatasetFactory factory = new DefaultMetricDatasetFactory(cConf, datasetFramework);
    DefaultMetricDatasetFactory.setupDatasets(factory);

    // Usage registry
    UsageRegistry.setupDatasets(datasetFramework);
  }
}
