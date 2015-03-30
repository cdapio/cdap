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

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.TwillModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.config.ConfigStore;
import co.cask.cdap.config.DefaultConfigStore;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeClassLoaderFactory;
import co.cask.cdap.data2.datafabric.dataset.type.DistributedDatasetTypeClassLoaderFactory;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.schedule.ScheduledRuntime;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.store.ScheduleStoreTableUtil;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.distributed.TransactionService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Command line tool for the Upgrade tool
 */
public class UpgradeTool {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeTool.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TransactionService txService;
  private final ZKClientService zkClientService;
  private final Injector injector;
  private final HBaseTableUtil hBaseTableUtil;
  private final NamespacedLocationFactory namespacedLocationFactory;

  private Store store;
  private FileMetaDataManager fileMetaDataManager;

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrades CDAP from 2.6 to 2.8\n" +
              "  This will upgrade CDAP from 2.6 to 2.8 version. \n" +
              "  The upgrade tool upgrades the following: \n" +
              "  1. User Datasets (Upgrades only the coprocessor jars)\n" +
              "  2. System Datasets\n" +
              "  3. Dataset Type and Instance Metadata\n" +
              "  4. Application Metadata\n" +
              "  5. Archives and Files\n" +
              "  6. Logs Metadata\n" +
              "  7. Stream state store table\n" +
              "  8. Queue config table\n" +
              "  9. Metrics Kafka table\n" +
              "  Note: Once you run the upgrade tool you cannot rollback to the previous version."),
    HELP("Show this help.");

    private final String description;

    private Action(String description) {
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
    this.hBaseTableUtil = injector.getInstance(HBaseTableUtil.class);
    this.namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

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
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new DataFabricDistributedModule());
          // the DataFabricDistributedModule needs MetricsCollectionService binding and since Upgrade tool does not do
          // anything with Metrics we just bind it to NoOpMetricsCollectionService
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          bind(Scheduler.class).toInstance(createNoopScheduler());
          bind(DatasetTypeClassLoaderFactory.class).to(DistributedDatasetTypeClassLoaderFactory.class);
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class);
          bind(MetricDatasetFactory.class).to(DefaultMetricDatasetFactory.class).in(Scopes.SINGLETON);
          bind(MetricStore.class).to(DefaultMetricStore.class);
          install(new FactoryModuleBuilder()
                    .implement(Store.class, DefaultStore.class)
                    .build(StoreFactory.class)
          );
          bind(ConfigStore.class).to(DefaultConfigStore.class);
        }

        @Provides
        @Singleton
        public DatasetFramework getDSFramework(
          CConfiguration cConf, DatasetDefinitionRegistryFactory registryFactory,
          @Named("defaultDatasetModules") Map<String, DatasetModule> defaultModules)
          throws IOException, DatasetManagementException {
          return createRegisteredDatasetFramework(cConf, registryFactory, defaultModules);
        }

        @Provides
        @Singleton
        @Named("defaultStore")
        public Store getStore(DatasetFramework dsFramework,
                              CConfiguration cConf, LocationFactory locationFactory,
                              TransactionExecutorFactory txExecutorFactory) {
          return new DefaultStore(cConf, locationFactory, namespacedLocationFactory, txExecutorFactory, dsFramework);
        }

        @Provides
        @Singleton
        @Named("logSaverTableUtil")
        public LogSaverTableUtil getLogSaverTableUtil(DatasetFramework dsFramework,
                                                      CConfiguration cConf) {
          return new LogSaverTableUtil(dsFramework, cConf);
        }

        @Provides
        @Singleton
        @Named("fileMetaDataManager")
        public FileMetaDataManager getFileMetaDataManager(@Named("logSaverTableUtil") LogSaverTableUtil tableUtil,
                                                          DatasetFramework dsFramework,
                                                          TransactionExecutorFactory txExecutorFactory,
                                                          LocationFactory locationFactory) {
          return new FileMetaDataManager(tableUtil, txExecutorFactory, locationFactory, namespacedLocationFactory,
                                         dsFramework, cConf);
        }
      });
  }

  private Scheduler createNoopScheduler() {
    return new Scheduler() {
      @Override
      public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule) {
      }

      @Override
      public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules) {
      }

      @Override
      public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType) {
        return ImmutableList.of();
      }

      @Override
      public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType) {
        return ImmutableList.of();
      }

      @Override
      public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule) {

      }

      @Override
      public void deleteSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void deleteSchedules(Id.Program programId, SchedulableProgramType programType) {
      }

      @Override
      public void deleteAllSchedules(Id.Namespace namespaceId)
        throws co.cask.cdap.internal.app.runtime.schedule.SchedulerException {
      }

      @Override
      public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName) {
        return ScheduleState.NOT_FOUND;
      }
    };
  }

  /**
   * Do the start up work
   */
  private void startUp() throws IOException {
    // Start all the services.
    zkClientService.startAndWait();
    txService.startAndWait();

    createNamespaces();
  }

  /**
   * Stop services and
   */
  private void stop() {
    try {
      txService.stopAndWait();
      zkClientService.stopAndWait();
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

    try {
      switch (action) {
        case UPGRADE:
          Scanner scan = new Scanner(System.in);
          System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
          System.out.println("Do you want to continue (y/n)");
          String response = scan.next();
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
      throw e;
    }
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
    HBaseAdmin hBaseAdmin = new HBaseAdmin(hConf);
    DatasetUpgrader dsUpgrade = injector.getInstance(DatasetUpgrader.class);
    dsUpgrade.upgrade();
    hBaseTableUtil.dropTable(hBaseAdmin, dsUpgrade.getDatasetInstanceMDSUpgrader().getOldDatasetInstanceTableId());
    hBaseTableUtil.dropTable(hBaseAdmin, dsUpgrade.getDatasetTypeMDSUpgrader().getOldDatasetTypeTableId());

    LOG.info("Upgrading application metadata ...");
    MDSUpgrader mdsUpgrader = injector.getInstance(MDSUpgrader.class);
    mdsUpgrader.upgrade();
    hBaseTableUtil.dropTable(hBaseAdmin, mdsUpgrader.getOldAppMetaTableId());

    LOG.info("Upgrading archives and files ...");
    ArchiveUpgrader archiveUpgrader = injector.getInstance(ArchiveUpgrader.class);
    archiveUpgrader.upgrade();

    LOG.info("Upgrading logs meta data ...");
    getFileMetaDataManager().upgrade();
    hBaseTableUtil.dropTable(hBaseAdmin, getFileMetaDataManager().getOldLogMetaTableId());

    LOG.info("Upgrading stream state store table ...");
    StreamStateStoreUpgrader streamStateStoreUpgrader = injector.getInstance(StreamStateStoreUpgrader.class);
    streamStateStoreUpgrader.upgrade();

    LOG.info("Upgrading queue.config table ...");
    QueueConfigUpgrader queueConfigUpgrader = injector.getInstance(QueueConfigUpgrader.class);
    queueConfigUpgrader.upgrade();

    LOG.info("Upgrading metrics.kafka.meta table ...");
    MetricsKafkaUpgrader metricsKafkaUpgrader = injector.getInstance(MetricsKafkaUpgrader.class);
    if (metricsKafkaUpgrader.tableExists()) {
      metricsKafkaUpgrader.upgrade();
      hBaseTableUtil.dropTable(hBaseAdmin, metricsKafkaUpgrader.getOldKafkaMetricsTableId());
    }
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
  private DatasetFramework createRegisteredDatasetFramework(CConfiguration cConf,
                                                            DatasetDefinitionRegistryFactory registryFactory,
                                                            Map<String, DatasetModule> defaultModules)
    throws DatasetManagementException, IOException {
    DatasetFramework datasetFramework = new InMemoryDatasetFramework(registryFactory, defaultModules, cConf);
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

    return datasetFramework;
  }

  /**
   * Creates the {@link Constants#SYSTEM_NAMESPACE} in hbase and {@link Constants#DEFAULT_NAMESPACE} namespace and also
   * adds it to the store
   */
  private void createNamespaces() throws IOException {
    LOG.info("Creating {} namespace in hbase", Constants.SYSTEM_NAMESPACE_ID);
    try {
      HBaseAdmin admin = new HBaseAdmin(hConf);
      hBaseTableUtil.createNamespaceIfNotExists(admin, Constants.SYSTEM_NAMESPACE_ID);
    } catch (MasterNotRunningException e) {
      Throwables.propagate(e);
    } catch (ZooKeeperConnectionException e) {
      Throwables.propagate(e);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    LOG.info("Creating and registering {} namespace", Constants.DEFAULT_NAMESPACE);
    getStore().createNamespace(Constants.DEFAULT_NAMESPACE_META);
    namespacedLocationFactory.get(Constants.DEFAULT_NAMESPACE_ID).mkdirs();
  }

  /**
   * gets the Store to access the app meta table
   *
   * @return {@link Store}
   */
  private Store getStore() {
    if (store == null) {
      store = injector.getInstance(Key.get(Store.class, Names.named("defaultStore")));
    }
    return store;
  }

  /**
   * gets the {@link FileMetaDataManager} to update log meta
   *
   * @return {@link FileMetaDataManager}
   */
  private FileMetaDataManager getFileMetaDataManager() {
    if (fileMetaDataManager == null) {
      fileMetaDataManager = injector.getInstance(Key.get(FileMetaDataManager.class,
                                                         Names.named("fileMetaDataManager")));
    }
    return fileMetaDataManager;
  }
}
