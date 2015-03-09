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
import co.cask.cdap.data2.dataset2.DatasetNamespace;
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
import co.cask.cdap.metrics.store.upgrade.UpgradeMetricsConstants;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.sun.tools.javac.resources.version;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Command line tool for data migration.
 */
public class DataMigration {
  private static final String KEEP_OLD_METRICS_TABLES = "--keep-old-metrics-tables";
  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    // NOTE : Metrics migration is not required for CDAP to work after upgrade,
    // some may opt not to migrate data as it may take a while. Hence, it is extracted as separate command/step
    METRICS("Migrate metrics data, to preserve old table data use option --keep-old-metrics-tables"),
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
          boolean keepOldTable = false;
          if (args.length == 2) {
            keepOldTable = parseArgument(args[1]);
          }
          migrateMetricsData(injector, keepOldTable);
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

  private boolean parseArgument(String arg) {
    if (arg != null && arg.equals(KEEP_OLD_METRICS_TABLES)) {
      return true;
    }
    return false;
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


  private void migrateMetricsData(Injector injector, boolean keepOldTables) throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    Configuration hConf = injector.getInstance(Configuration.class);

    System.out.println("Starting metrics data migration");
    // find version to migrate
    MetricHBaseTableUtil metricHBaseTableUtil = new MetricHBaseTableUtil(injector.getInstance(HBaseTableUtil.class));
    Version version = findMetricsTableVersion(injector.getInstance(CConfiguration.class),
                                              injector.getInstance(Configuration.class), metricHBaseTableUtil);

    if (version != null) {
      System.out.println("Migrating Metrics Data from " + version.name());
      DatasetFramework framework = createRegisteredDatasetFramework(injector);
      // perform sanity check - truncate 2.8 tables data
      truncateV2Tables(cConf, hConf);
      // migrate metrics data
      DefaultMetricDatasetFactory.migrateData(injector.getInstance(CConfiguration.class), framework, version);
      System.out.println ("Keep Old Table Flag is : " + keepOldTables);
      // delete old-metrics table identified by version, unless keepOldTables flag is true
      if (!keepOldTables) {
        cleanUpOldTables(cConf, hConf, version);
      }
    }
  }

  private void truncateV2Tables(CConfiguration cConf, Configuration hConf) {
    String rootPrefix = cConf.get(Constants.Dataset.TABLE_PREFIX) + "_";
    String v2EntityTableName =  cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                          MetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
    v2EntityTableName = getTableName(rootPrefix, Id.DatasetInstance.from(
      Id.Namespace.from(Constants.SYSTEM_NAMESPACE), v2EntityTableName));
    String v2MetricsTablePrefix =  cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                             MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX);
    v2MetricsTablePrefix = getTableName(rootPrefix, Id.DatasetInstance.from(
      Id.Namespace.from(Constants.SYSTEM_NAMESPACE), v2MetricsTablePrefix));
    HBaseAdmin hAdmin;
    try {
      hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (desc.getNameAsString().equals(v2EntityTableName) ||
          desc.getNameAsString().startsWith(v2MetricsTablePrefix)) {
          System.out.println(String.format("Deleting table %s before upgrade", desc.getNameAsString()));
          //disable the table
          hAdmin.disableTable(desc.getName());
          //delete the table
          hAdmin.deleteTable(desc.getName());
        }
      }
    } catch (Exception e) {
      System.out.println("Unable to truncate desitation tables : " + e);
    }
  }

  private void cleanUpOldTables(CConfiguration cConf, Configuration hConf, Version version) {
    Set<String> tablesToDelete = Sets.newHashSet();
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);

    // add kafka meta table to deleteList
    tablesToDelete.add(addNamespace(defaultDatasetNamespace, cConf.get(MetricsConstants.ConfigKeys.KAFKA_META_TABLE,
                                                                       MetricsConstants.DEFAULT_KAFKA_META_TABLE)));

    if (version == Version.VERSION_2_6_OR_LOWER) {
      List<String> scopes = ImmutableList.of("system", "user");
      // add user and system - entity tables , aggregates table and time series table to the list
      for (String scope : scopes) {
        addTableNamesToDelete(tablesToDelete, cConf, scope, ImmutableList.of(1));
      }
    }

    if (version == Version.VERSION_2_7) {
      addTableNamesToDelete(tablesToDelete, cConf, null, ImmutableList.of(1, 60, 3600));
    }

    System.out.println("Deleting Tables : " + tablesToDelete);
    deleteTables(hConf, tablesToDelete);
  }

  private void deleteTables(Configuration hConf, Set<String> tablesToDelete) {
    HBaseAdmin hAdmin = null;
    try {
      hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (tablesToDelete.contains(desc.getNameAsString())) {
          // disable the table
          hAdmin.disableTable(desc.getName());
          // delete the table
          hAdmin.deleteTable(desc.getName());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void addTableNamesToDelete(Set<String> tablesToDelete, CConfiguration cConf,
                                     String scope, List<Integer> resolutions) {
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);
    tablesToDelete.add(addNamespace(defaultDatasetNamespace, scope, cConf.get(
      MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME, UpgradeMetricsConstants.DEFAULT_ENTITY_TABLE_NAME)));
    String tableNamePrefix = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                       UpgradeMetricsConstants.DEFAULT_METRICS_TABLE_PREFIX);
    // add aggregates table
    tablesToDelete.add(addNamespace(defaultDatasetNamespace, scope, tableNamePrefix + ".agg"));

    // add timeseries tables
    for (int resolution : resolutions) {
      tablesToDelete.add(addNamespace(defaultDatasetNamespace, scope, tableNamePrefix + ".ts." + resolution));
    }
  }

  private String getTableName(String rootPrefix, Id.DatasetInstance instance) {
    return  rootPrefix + instance.getNamespaceId() + ":" + instance.getId();
  }
  private String addNamespace(DatasetNamespace dsNamespace, String tableName) {
    return addNamespace(dsNamespace, null , tableName);
  }
  private String addNamespace(DatasetNamespace dsNamespace, String scope, String tableName) {
    tableName = scope == null ? tableName : scope + "." + tableName;
    return dsNamespace.namespace(new Id.Namespace(Constants.SYSTEM_NAMESPACE), tableName);
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
    HBaseAdmin hAdmin = null;
    Version version = null;
    try {
      hAdmin = new HBaseAdmin(hConf);
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
