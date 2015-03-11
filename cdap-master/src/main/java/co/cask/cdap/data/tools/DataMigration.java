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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.upgrade.DataMigrationException;
import co.cask.cdap.proto.Id;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * Command line tool to migrate data between different versions of CDAP.
 * Usually used along with upgrade tool{@link UpgraderMain}
 */
public class DataMigration {
  private static final String KEEP_OLD_METRICS_DATA = "--keep-old-metrics-data";

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    METRICS("Migrate metrics data, to preserve old table data use option " + KEEP_OLD_METRICS_DATA),
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

    MigrationAction action = getAction(args);
    if (action == null) {
      return;
    }

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
          bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
        }
      });
    action.perform(injector);
  }

  public MigrationAction getAction(String[] args) {
    if (args.length < 1) {
      printHelp();
      return null;
    }
    Action action = parseAction(args[0]);
    if (action == null) {
      System.out.println(String.format("Unsupported action : %s", args[0]));
      printHelp(true);
      return null;
    }
    switch (action) {
      case METRICS:
        return getMetricsMigrationAction(args);
      case HELP:
        printHelp();
        break;
    }
    return null;
  }

  private MigrationAction getMetricsMigrationAction(String[] args) {
    if (args.length > 2) {
      System.out.println("invalid number of arguments");
      printHelp(true);
    } else if (args.length == 2) {
      if (args[1].equals(KEEP_OLD_METRICS_DATA)) {
        return new MetricsMigration(true);
      } else {
        System.out.println("invalid argument, expected argument " + KEEP_OLD_METRICS_DATA);
      }
    } else {
      return new MetricsMigration(false);
    }
    return null;
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

  private static final class MetricsMigration implements MigrationAction {

    boolean keepOldMetricsData;

    public MetricsMigration(boolean keepOldMetricsData) {
      this.keepOldMetricsData = keepOldMetricsData;
    }

    @Override
    public void perform(Injector injector) {
      CConfiguration cConf = injector.getInstance(CConfiguration.class);
      Configuration hConf = injector.getInstance(Configuration.class);

      try {
        DatasetFramework framework = createRegisteredDatasetFramework(injector);
        // migrate metrics data
        DefaultMetricDatasetFactory.migrateData(cConf, hConf, framework, keepOldMetricsData,
                                                injector.getInstance(HBaseTableUtil.class));
      } catch (DataMigrationException e) {
        System.out.println(
          String.format("Exception encountered during metrics migration : %s , Aborting metrics data migration",
                        e.getMigrationExceptionMessage()));
      } catch (Exception e) {
        System.out.println(String.format(
          "Exception encountered : %s , Aborting metrics migration", e));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new DataMigration().doMain(args);
  }

  private static interface MigrationAction {
    void perform(Injector injector);
  }

  public static MigrationAction testMigrationParsing(String[] args) {
    return new DataMigration().getMetricsMigrationAction(args);
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   */
  public static DatasetFramework createRegisteredDatasetFramework(Injector injector)
    throws DatasetManagementException, IOException {
    DatasetDefinitionRegistryFactory registryFactory = injector.getInstance(DatasetDefinitionRegistryFactory.class);
    DatasetFramework datasetFramework =
      new InMemoryDatasetFramework(registryFactory, injector.getInstance(CConfiguration.class));
    // TODO: this doesn't sound right. find out why its needed.
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "table"),
                               new HBaseTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "metricsTable"),
                               new HBaseMetricsTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "core"), new CoreDatasetsModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "fileSet"), new FileSetModule());
    return datasetFramework;
  }
}
