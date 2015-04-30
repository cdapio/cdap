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

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.TwillModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.tools.flow.FlowQueuePendingCorrector;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.schedule.AbstractSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import co.cask.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.templates.AdapterDefinition;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.distributed.TransactionService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.ZKClientService;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final FlowQueuePendingCorrector flowQueuePendingCorrector;

  private Store store;
  QuartzScheduler qs;
  private final AdapterService adapterService;
  private final DatasetFramework dsFramework;
  private DatasetBasedTimeScheduleStore datasetBasedTimeScheduleStore;

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrades CDAP to 3.0\n" +
              "  The upgrade tool upgrades the following: \n" +
              "  1. User Datasets (Upgrades only the coprocessor jars)\n" +
              "  2. System Datasets\n" +
              "  3. StreamConversionAdapter\n" +
              "  4. queue.pending metrics for flowlets\n" +
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
    this.namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    this.dsFramework = injector.getInstance(DatasetFramework.class);
    this.adapterService = injector.getInstance(AdapterService.class);
    this.flowQueuePendingCorrector = injector.getInstance(FlowQueuePendingCorrector.class);

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
      new IOModule(),
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new TwillModule(),
      new AuthModule(),
      new ExploreClientModule(),
      new DataFabricDistributedModule(),
      new ServiceStoreModules().getDistributedModule(),
      new DataSetsModules().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new KafkaClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
          bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
        }

        @Provides
        @Singleton
        @Named("defaultStore")
        public Store getStore(DatasetFramework dsFramework,
                              CConfiguration cConf, LocationFactory locationFactory,
                              TransactionExecutorFactory txExecutorFactory) {
          return new DefaultStore(cConf, locationFactory, namespacedLocationFactory, txExecutorFactory, dsFramework);
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
  private void startUp() throws IOException, DatasetManagementException {
    // Start all the services.
    zkClientService.startAndWait();
    txService.startAndWait();
    flowQueuePendingCorrector.startAndWait();
  }

  /**
   * Stop services and
   */
  private void stop() {
    try {
      txService.stopAndWait();
      zkClientService.stopAndWait();
      if (qs != null) {
        qs.shutdown();
      }
      flowQueuePendingCorrector.stopAndWait();
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
    LOG.info("Upgrading System and User Datasets ...");
    DatasetUpgrader dsUpgrade = injector.getInstance(DatasetUpgrader.class);
    dsUpgrade.upgrade();

    upgradeAdapters();
    populateFlowQueuePendingMetrics();
  }

  private void populateFlowQueuePendingMetrics() throws Exception {
    NamespaceAdmin namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    List<NamespaceMeta> namespaceMetas = namespaceAdmin.listNamespaces();
    for (NamespaceMeta namespaceMeta : namespaceMetas) {
      Id.Namespace namespaceId = Id.Namespace.from(namespaceMeta.getName());
      Collection<ApplicationSpecification> apps = getStore()
        .getAllApplications(namespaceId);
      for (ApplicationSpecification app : apps) {
        Id.Application appId = Id.Application.from(namespaceId, app.getName());
        for (FlowSpecification flow : app.getFlows().values()) {
          flowQueuePendingCorrector.run(Id.Flow.from(appId, flow.getName()));
        }
      }
    }
  }

  /**
   * Upgrades adapters by first deleting all the schedule triggers and the the job itself in every namespace follwed by
   * all the adapters in the namespace
   *
   * @throws SchedulerException
   */
  private void upgradeAdapters() throws SchedulerException {
    DatasetBasedTimeScheduleStore datasetBasedTimeScheduleStore = getDatasetBasedTimeScheduleStore();
    NamespaceAdmin namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    List<NamespaceMeta> namespaceMetas = namespaceAdmin.listNamespaces();
    for (NamespaceMeta namespaceMeta : namespaceMetas) {
      String namespace = namespaceMeta.getName();
      Collection<AdapterDefinition> adapters = adapterService.getAdapters(Id.Namespace
                                                                            .from(namespace));
      Id.Program program = Id.Program.from(namespace, "stream-conversion", ProgramType.WORKFLOW,
                                           "StreamConversionWorkflow");
      for (AdapterDefinition adapter : adapters) {
        TriggerKey triggerKey = new TriggerKey(AbstractSchedulerService.scheduleIdFor(
          program, SchedulableProgramType.WORKFLOW, adapter.getName() + "StreamConversionWorkflow"));
        if (datasetBasedTimeScheduleStore.removeTrigger(triggerKey)) {
          LOG.info("Removed adapter trigger: {}", triggerKey.toString());
          getStore().removeAdapter(Id.Namespace.from(namespace), adapter.getName());
        }
      }
      //delete the stream-conversion job entry
      datasetBasedTimeScheduleStore.removeJob(new JobKey(AbstractSchedulerService
                                                           .programIdFor(program, SchedulableProgramType.WORKFLOW)));
    }
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
   * gets the {@link DatasetBasedTimeScheduleStore} which stores the schedules of adapters
   *
   * @return {@link DatasetBasedTimeScheduleStore}
   * @throws SchedulerException
   */
  private DatasetBasedTimeScheduleStore getDatasetBasedTimeScheduleStore() throws SchedulerException {
    if (datasetBasedTimeScheduleStore == null) {
      datasetBasedTimeScheduleStore = injector.getInstance(DatasetBasedTimeScheduleStore.class);
      // need to call initialize on datasetBasedTimeScheduleStore
      ExecutorThreadPool threadPool = new ExecutorThreadPool(1);
      threadPool.initialize();
      String schedulerName = DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME;
      String schedulerInstanceId = DirectSchedulerFactory.DEFAULT_INSTANCE_ID;

      QuartzSchedulerResources qrs = new QuartzSchedulerResources();
      JobRunShellFactory jrsf = new StdJobRunShellFactory();

      qrs.setName(schedulerName);
      qrs.setInstanceId(schedulerInstanceId);
      qrs.setJobRunShellFactory(jrsf);
      qrs.setThreadPool(threadPool);
      qrs.setThreadExecutor(new DefaultThreadExecutor());
      qrs.setJobStore(datasetBasedTimeScheduleStore);
      qrs.setRunUpdateCheck(false);
      qs = new QuartzScheduler(qrs, -1, -1);
      ClassLoadHelper cch = new CascadingClassLoadHelper();
      cch.initialize();

      datasetBasedTimeScheduleStore.initialize(cch, qs.getSchedulerSignaler());
    }
    return datasetBasedTimeScheduleStore;
  }

  public static void main(String[] args) {
    try {
      UpgradeTool upgradeTool = new UpgradeTool();
      upgradeTool.doMain(args);
    } catch (Throwable t) {
      LOG.error("Failed to upgrade ...", t);
    }
  }
}
