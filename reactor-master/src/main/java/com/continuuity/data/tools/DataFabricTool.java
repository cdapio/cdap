/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.utils.ProjectInfo;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;
import java.util.Properties;

/**
 * Data-Fabric command line tool.
 */
public class DataFabricTool {

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
            bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
          }
        },
        new ZKClientModule(),
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

    // Upgrade all user tables.
    Properties properties = new Properties();
    for (Map.Entry<String, Class<?>> entry : accessor.list(DataSetAccessor.Namespace.USER).entrySet()) {
      DataSetManager manager = accessor.getDataSetManager(entry.getValue(), DataSetAccessor.Namespace.USER);
      manager.upgrade(entry.getKey(), properties);
    }

    // Upgrade all queue and stream tables.
    queueAdmin.upgrade();
    streamAdmin.upgrade();

    // Upgrade metrics table
    metricsTableFactory.upgrade();
  }

  public static void main(String[] args) throws Exception {
    new DataFabricTool().doMain(args);
  }
}
