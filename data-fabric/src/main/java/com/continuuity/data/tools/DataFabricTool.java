/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.utils.ProjectInfo;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Data-Fabric command line tool.
 */
public class DataFabricTool {

  private static final Logger LOG = LoggerFactory.getLogger(DataFabricTool.class);

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
    }

    try {
      CConfiguration cConf = CConfiguration.create();
      Configuration hConf = HBaseConfiguration.create();

      Injector injector = Guice.createInjector(
        new ConfigModule(cConf, hConf),
        new LocationRuntimeModule().getDistributedModules(),
        new DataFabricModules(cConf, hConf).getDistributedModules()
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
      LOG.error("Failed to perform action '{}'. Reason: {}.", action, e.getMessage(), e);
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

    // Upgrade all user tables.
    for (Map.Entry<String, Class<?>> entry : accessor.list(DataSetAccessor.Namespace.USER).entrySet()) {
      DataSetManager manager = accessor.getDataSetManager(entry.getValue(), DataSetAccessor.Namespace.USER);
      manager.upgrade(entry.getKey());
    }

    // Upgrade all queue and stream tables.
    queueAdmin.upgrade();
    streamAdmin.upgrade();

    // Upgrade metrics table
    // TODO: It is hacky to match with ".metrics." in the table name.
    // It's because we only have SYSTEM and USER namespace while different usage of table subdivide the
    // table namespaces by themselves.
    // Ideally, Namespace is a prefix string so that the list could match with that.
    for (Map.Entry<String, Class<?>> entry : accessor.list(DataSetAccessor.Namespace.SYSTEM).entrySet()) {
      if (entry.getKey().contains(".metrics.")) {
        DataSetManager manager = accessor.getDataSetManager(MetricsTable.class,
                                                            DataSetAccessor.Namespace.SYSTEM);
        manager.upgrade(entry.getKey());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new DataFabricTool().doMain(args);
  }
}
