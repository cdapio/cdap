/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data.DataSetAccessor;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.dataset.api.DataSetManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.runtime.schedule.ScheduleStoreTableUtil;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.metrics.data.DefaultMetricsTableFactory;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;
import java.util.Properties;

/**
 * Reactor command line tool.
 */
public class ReactorTool {

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrade all tables."),
    HELP("Show this help.");

    private final String description;

    private Action(String description) {
      this.description = description;
    }

    private String getDescription() {
      return description;
    }
  }

  public void doMain(String[] args) throws Exception {
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
      CConfiguration cConf = CConfiguration.create();
      Configuration hConf = HBaseConfiguration.create();

      Injector injector = Guice.createInjector(
        new ConfigModule(cConf, hConf),
        new LocationRuntimeModule().getDistributedModules(),
        new DataFabricModules().getDistributedModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            install(new FactoryModuleBuilder()
                      .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                      .build(DatasetDefinitionRegistryFactory.class));
          }
        },
        new KafkaClientModule(),
        new MetricsClientRuntimeModule().getDistributedModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
          }
        },
        new ZKClientModule(),
        new IOModule(),
        new AuthModule(),
        new DiscoveryRuntimeModule().getDistributedModules()
      );

      switch (action) {
        case UPGRADE:
          performUpgrade(injector);
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
      System.out.println(String.format("  %s - %s", action.name().toLowerCase(), action.getDescription()));
    }
  }

  private Action parseAction(String action) {
    try {
      return Action.valueOf(action.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private void performUpgrade(Injector injector) throws Exception {
    DataSetAccessor accessor = injector.getInstance(DataSetAccessor.class);
    QueueAdmin queueAdmin = injector.getInstance(QueueAdmin.class);
    StreamAdmin streamAdmin = injector.getInstance(StreamAdmin.class);
    MetricsTableFactory metricsTableFactory = injector.getInstance(MetricsTableFactory.class);
    LogSaverTableUtil logSaverUtil = injector.getInstance(LogSaverTableUtil.class);
    ScheduleStoreTableUtil scheduleStoreUtil = injector.getInstance(ScheduleStoreTableUtil.class);

    DatasetFramework framework = getDatasetFramework(injector);
    DatasetMetaTableUtil datasetTableUtil = new DatasetMetaTableUtil(framework);
    datasetTableUtil.init();

    // Upgrade all user tables.
    Properties properties = new Properties();
    for (Map.Entry<String, Class<?>> entry : accessor.list(DataSetAccessor.Namespace.USER).entrySet()) {
      DataSetManager manager = accessor.getDataSetManager(entry.getValue(), DataSetAccessor.Namespace.USER);
      manager.upgrade(entry.getKey(), properties);
    }

    // Upgrade all queue and stream tables.
    queueAdmin.upgrade();
    streamAdmin.upgrade();

    // Upgrade app mds datasets
    DefaultStore.upgrade(framework);

    // Upgrade schedule store
    scheduleStoreUtil.upgrade();

    // Upgrade log saver meta table
    logSaverUtil.upgrade();

    // Upgrade the dataset types table
    datasetTableUtil.upgrade();

    // Upgrade metrics table
    metricsTableFactory.upgrade();
  }

  private DatasetFramework getDatasetFramework(Injector injector) throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);

    DatasetDefinitionRegistryFactory factory = injector.getInstance(DatasetDefinitionRegistryFactory.class);
    return DatasetMetaTableUtil.createRegisteredDatasetFramework(factory, cConf);
  }

  public static void main(String[] args) throws Exception {
    new ReactorTool().doMain(args);
  }
}
