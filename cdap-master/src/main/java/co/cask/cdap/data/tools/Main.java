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

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.config.DefaultConfigStore;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.data2.util.hbase.TableId;
import co.cask.cdap.internal.app.runtime.schedule.store.ScheduleStoreTableUtil;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.save.LogSaverTableUtil;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Command line tool.
 */
public class Main {
  private static final String KEEP_OLD_METRICS_TABLES = "--keep-old-metrics-tables";
  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrade all tables."),
    MIGRATE_METRICS_DATA("Migrate metrics data to CDAP-2.8"),
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
        case UPGRADE:
          performUpgrade(injector);
        break;
        case MIGRATE_METRICS_DATA:
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

  private void performUpgrade(Injector injector) throws Exception {
    // Upgrade system datasets
    upgradeSystemDatasets(injector);

    // Upgrade all user hbase tables
    upgradeUserTables(injector);

    // Upgrade all queue and stream tables.
    QueueAdmin queueAdmin = injector.getInstance(QueueAdmin.class);
    queueAdmin.upgrade();
  }

  private void upgradeSystemDatasets(Injector injector) throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    // Setting up all system datasets to be upgraded, collecting them from respective components
    DatasetFramework framework = createRegisteredDatasetFramework(injector);
    // dataset service
    DatasetMetaTableUtil.setupDatasets(framework);
    // app metadata
    DefaultStore.setupDatasets(framework);
    // config store
    DefaultConfigStore.setupDatasets(framework);
    // logs metadata
    LogSaverTableUtil.setupDatasets(framework);
    // scheduler metadata
    ScheduleStoreTableUtil.setupDatasets(framework);
    // metrics data
    DefaultMetricDatasetFactory.setupDatasets(cConf, framework);

    // Upgrade all datasets in system namespace
    Id.Namespace systemNamespace = Id.Namespace.from(Constants.SYSTEM_NAMESPACE);
    for (DatasetSpecification spec : framework.getInstances(systemNamespace)) {
      System.out.println(String.format("Upgrading dataset: %s, spec: %s", spec.getName(), spec.toString()));
      DatasetAdmin admin = framework.getAdmin(Id.DatasetInstance.from(systemNamespace, spec.getName()), null);
      // we know admin is not null, since we are looping over existing datasets
      admin.upgrade();
      System.out.println(String.format("Upgraded dataset: %s", spec.getName()));
    }
  }

  private void migrateMetricsData(Injector injector, boolean keepOldTables) throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    Configuration hConf = injector.getInstance(Configuration.class);
    // find version to migrate
    ProjectInfo.Version version = findMetricsTableVersion(cConf, hConf);
    if (version != null) {
      DatasetFramework framework = createRegisteredDatasetFramework(injector);
      // perform sanity check - truncate 2.8 tables data
      truncateV2Tables(cConf, hConf);
      // migrate metrics data
      DefaultMetricDatasetFactory.migrateMetricsData(injector.getInstance(CConfiguration.class), framework, version);
      System.out.println ("Keep Old Table Flag is" + keepOldTables);
      // delete old-metrics table identified by version, unless keepOldTables flag is true
      if (!keepOldTables) {
        cleanUpOldTables(cConf, hConf, version);
      }
    }
  }

  private void truncateV2Tables(CConfiguration cConf, Configuration hConf) {
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);
    String v2EntityTableName =  cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                          MetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
    v2EntityTableName = addNamespace(defaultDatasetNamespace, v2EntityTableName);
    String v2MetricsTablePrefix =  cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                             MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX);
    v2MetricsTablePrefix = addNamespace(defaultDatasetNamespace, v2MetricsTablePrefix);
    HBaseAdmin hAdmin = null;
    try {
      hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (desc.getNameAsString().equals(v2EntityTableName) ||
          desc.getNameAsString().startsWith(v2MetricsTablePrefix)) {
          System.out.println(String.format("Deleting table %s before upgrade", desc.getNameAsString()));
          // disable the table
          //hAdmin.disableTable(desc.getName());
          // delete the table
          //hAdmin.deleteTable(desc.getName());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void cleanUpOldTables(CConfiguration cConf, Configuration hConf, ProjectInfo.Version version) {
    Set<String> tablesToDelete = Sets.newHashSet();
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);

    // add kafka meta table to deleteList
    tablesToDelete.add(addNamespace(defaultDatasetNamespace, cConf.get(MetricsConstants.ConfigKeys.KAFKA_META_TABLE,
                                                                       MetricsConstants.DEFAULT_KAFKA_META_TABLE)));

    if (version.getMajor() == 2 && version.getMinor() <= 6) {
      List<String> scopes = ImmutableList.of("system", "user");
      // add user and system - entity tables , aggregates table and time series table to the list
      for (String scope : scopes) {
        addTableNamesToDelete(tablesToDelete, cConf, scope, ImmutableList.of(1));
      }
    }

    if (version.getMajor() == 2 && version.getMinor() == 7) {
      addTableNamesToDelete(tablesToDelete, cConf, null, ImmutableList.of(1, 60, 3600));
    }

    System.out.println("Deleting Tables : " + tablesToDelete);
    //deleteTables(hConf, tablesToDelete);

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

  private String addNamespace(DatasetNamespace dsNamespace, String tableName) {
    return addNamespace(dsNamespace, null , tableName);
  }
  private String addNamespace(DatasetNamespace dsNamespace, String scope, String tableName) {
    tableName = scope == null ? tableName : scope + "." + tableName;
    return dsNamespace.namespace(new Id.Namespace(Constants.SYSTEM_NAMESPACE), tableName);
  }

  private ProjectInfo.Version findMetricsTableVersion(CConfiguration cConf, Configuration hConf) {
    // Upgrade all datasets in system namespace
    boolean metricsTable27Found = false;
    boolean metricsTable26Found = false;
    // versions older than 2.7, has two metrics table, identified by system and user prefix
    String tableName26 = cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                   "system." + UpgradeMetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
    String tableName27 = cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                   UpgradeMetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
    DefaultDatasetNamespace defaultDatasetNamespace = new DefaultDatasetNamespace(cConf);

    String metricsEntityTable26 = defaultDatasetNamespace.namespace(
      new Id.Namespace(Constants.SYSTEM_NAMESPACE), tableName26);
    String metricsEntityTable27 = defaultDatasetNamespace.namespace(
      new Id.Namespace(Constants.SYSTEM_NAMESPACE), tableName27);

    HBaseAdmin hAdmin = null;
    try {
      hAdmin = new HBaseAdmin(hConf);
      for (HTableDescriptor desc : hAdmin.listTables()) {
        if (desc.getNameAsString().equals(metricsEntityTable26)) {
          metricsTable26Found = true;
        } else if (desc.getNameAsString().equals(metricsEntityTable27)) {
          metricsTable27Found = true;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (metricsTable27Found) {
      System.out.println(" FOUND CDAP VERSION 2.7");
      return new ProjectInfo.Version("2.7.0");
    } else if (metricsTable26Found) {
      System.out.println(" FOUND CDAP VERSION 2.6");
      return new ProjectInfo.Version("2.6.0");
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    new Main().doMain(args);
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   */
  public static DatasetFramework createRegisteredDatasetFramework(Injector injector)
    throws DatasetManagementException, IOException {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);

    DatasetDefinitionRegistryFactory registryFactory = injector.getInstance(DatasetDefinitionRegistryFactory.class);
    DatasetFramework datasetFramework =
      new NamespacedDatasetFramework(new InMemoryDatasetFramework(registryFactory),
                                     new DefaultDatasetNamespace(cConf));
    // TODO: this doesn't sound right. find out why its needed.
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "table"),
                               new HBaseTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "metricsTable"),
                               new HBaseMetricsTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "core"), new CoreDatasetsModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "fileSet"), new FileSetModule());

    return datasetFramework;
  }

  private static void upgradeUserTables(final Injector injector) throws Exception  {
    // We assume that all tables in USER namespace belong to Table type datasets. So we loop thru them
    // and upgrading with the help of HBaseTableAdmin
    final CConfiguration cConf = injector.getInstance(CConfiguration.class);
    DefaultDatasetNamespace namespace = new DefaultDatasetNamespace(cConf);

    Configuration hConf = injector.getInstance(Configuration.class);
    HBaseAdmin hAdmin = new HBaseAdmin(hConf);
    final HBaseTableUtil hBaseTableUtil = injector.getInstance(HBaseTableUtil.class);

    for (HTableDescriptor desc : hAdmin.listTables()) {
      String tableName = desc.getNameAsString();
      TableId tableId = TableId.from(tableName);
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(tableId.getNamespace(), tableId.getTableName());
      // todo: it works now, but we will want to change it if namespacing of datasets in HBase is more than +prefix
      if (namespace.fromNamespaced(datasetInstanceId) != null) {
        System.out.println(String.format("Upgrading hbase table: %s, desc: %s", tableName, desc.toString()));

        final boolean supportsIncrement =
          "true".equalsIgnoreCase(desc.getValue(Table.PROPERTY_READLESS_INCREMENT));
        final boolean transactional =
          !"true".equalsIgnoreCase(desc.getValue(Constants.Dataset.TABLE_TX_DISABLED));
        DatasetAdmin admin = new AbstractHBaseDataSetAdmin(tableName, hConf, hBaseTableUtil) {
          @Override
          protected CoprocessorJar createCoprocessorJar() throws IOException {
            return HBaseTableAdmin.createCoprocessorJarInternal(cConf,
                injector.getInstance(LocationFactory.class),
                hBaseTableUtil,
                transactional,
                supportsIncrement);
          }

          @Override
          protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
            // we don't do any other changes apart from coprocessors upgrade
            return false;
          }

          @Override
          public void create() throws IOException {
            // no-op
            throw new UnsupportedOperationException("This DatasetAdmin is only used for upgrade() operation");
          }
        };
        admin.upgrade();
        System.out.println(String.format("Upgraded hbase table: %s", tableName));
      }
    }
  }
}
