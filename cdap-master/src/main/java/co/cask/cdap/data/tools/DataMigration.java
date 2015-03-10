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
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.lib.table.hbase.MetricHBaseTableUtil;
import co.cask.cdap.data2.dataset2.lib.table.hbase.MetricHBaseTableUtil.Version;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.upgrade.DataMigrationException;
import co.cask.cdap.metrics.store.upgrade.UpgradeMetricsConstants;
import co.cask.cdap.proto.Id;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Command line tool to migrate data between different versions of CDAP.
 * Usually used along with upgrade tool{@link UpgraderMain}
 */
public class DataMigration {
  private static final String KEEP_OLD_METRICS_DATA = "--keep-old-metrics-data";
  private static boolean isTest = false;

  private boolean keepOldMetricsData = false;

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

  public boolean doMain(String[] args) throws Exception {
    System.out.println(String.format("%s - version %s.", getClass().getSimpleName(), ProjectInfo.getVersion()));
    System.out.println();

    if (args.length < 1) {
      printHelp();
      return false;
    }

    Action action = parseAction(args[0]);
    if (action == null) {
      System.out.println(String.format("Unsupported action : %s", args[0]));
      printHelp(true);
      return false;
    }

    if (action.equals(Action.METRICS)) {
      if (args.length > 2) {
        System.out.println("invalid number of arguments");
        printHelp(true);
        return false;
      } else if (args.length == 2) {
        if (args[1].equals(KEEP_OLD_METRICS_DATA)) {
          keepOldMetricsData = true;
        } else {
          System.out.println("invalid argument, expected argument " + KEEP_OLD_METRICS_DATA);
          printHelp(true);
          return false;
        }
      }
    }

    if (isTest) {
      return true;
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

    switch (action) {
      case METRICS:
        migrateMetricsData(injector, keepOldMetricsData);
        break;
      case HELP:
        printHelp();
        break;
    }
    return true;
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

  private void migrateMetricsData(Injector injector, boolean keepOldTables) {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    Configuration hConf = injector.getInstance(Configuration.class);

    System.out.println("Starting metrics data migration");
    // find version to migrate
    MetricHBaseTableUtil metricHBaseTableUtil = new MetricHBaseTableUtil(injector.getInstance(HBaseTableUtil.class));
    Version version = findMetricsTableVersion(cConf, hConf, metricHBaseTableUtil);

    if (version != null) {
      System.out.println("Migrating Metrics Data from " + version.name());
      try {
        DatasetFramework framework = createRegisteredDatasetFramework(injector);
        // migrate metrics data
        DefaultMetricDatasetFactory.migrateData(cConf, hConf, framework, version, keepOldTables);
      } catch (DataMigrationException e) {
        System.out.println(
          String.format("Exception encountered during metrics migration : %s , Aborting metrics data migration",
                        e.getMigrationExceptionMessage()));
      } catch (Exception e) {
        System.out.println(String.format(
          "Exception encountered : %s , Aborting metrics migration", e));
      }
      System.out.println("Successfully Migrated Metrics Data from " + version.name());
    } else {
      System.out.println("Did not find compatible CDAP Version to migrate Metrics data from");
    }
  }

  @Nullable
  private Version findMetricsTableVersion(CConfiguration cConf, Configuration hConf,
                                          MetricHBaseTableUtil metricHBaseTableUtil) {


    // Figure out what is the latest working version of CDAP
    // 1) if latest is 2.8.x - nothing to do, if "pre-2.8", proceed to next step.
    // 2) find a most recent metrics table: start by looking for 2.7, then 2.6
    // 3) if we find 2.7 - we will migrate data from 2.7 table, if not - migrate data from 2.6 metrics table
    // todo - use UpgradeTool to figure out if version is 2.8.x, return if it is 2.8.x

    String tableName27 = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                   UpgradeMetricsConstants.DEFAULT_METRICS_TABLE_PREFIX) + ".agg";

    // versions older than 2.7, has two metrics table, identified by system and user prefix
    String tableName26 = "system." + tableName27;
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);
    String metricsEntityTable26 = defaultDatasetNamespace.namespace(new Id.Namespace(Constants.SYSTEM_NAMESPACE),
                                                                    tableName26);
    String metricsEntityTable27 = defaultDatasetNamespace.namespace(new Id.Namespace(Constants.SYSTEM_NAMESPACE),
                                                                    tableName27);

    Version version = null;
    try {
      HBaseAdmin  hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (desc.getNameAsString().equals(metricsEntityTable27)) {
          System.out.println("Matched HBase Table Name For Migration " + desc.getNameAsString());
          version = metricHBaseTableUtil.getVersion(desc);
          version = verifyVersion(Version.VERSION_2_7, version);
          if (version == null) {
            return null;
          }
          break;
        }
        if (desc.getNameAsString().equals(metricsEntityTable26)) {
          System.out.println("Matched HBase Table Name For Migration " + desc.getNameAsString());
          version = metricHBaseTableUtil.getVersion(desc);
          version = verifyVersion(Version.VERSION_2_6_OR_LOWER, version);
          if (version == null) {
            return null;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return version;
  }

  private Version verifyVersion(Version expected, Version actual) {
    if (expected != actual) {
      System.out.println("Version detected based on table name does not match table configuration");
      return null;
    }
    return actual;
  }

  public static void main(String[] args) throws Exception {
    new DataMigration().doMain(args);
  }

  public static boolean testMigrationParse(String[] args) throws Exception {
    isTest = true;
    return new DataMigration().doMain(args);
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
