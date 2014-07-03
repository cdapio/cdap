/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.tools;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.utils.ProjectInfo;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.datafabric.dataset.DatasetMetaTableUtil;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.app.runtime.schedule.ScheduleStoreTableUtil;
import com.continuuity.logging.save.LogSaverTableUtil;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
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
    MetaDataTable metaDataTable = injector.getInstance(MetaDataTable.class);
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

    // Upgrade the metadata table
    metaDataTable.upgrade();

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
