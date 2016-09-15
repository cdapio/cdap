/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.upgrade.DataMigrationException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Command line tool to migrate data between different versions of CDAP.
 * Usually used along with upgrade tool{@link UpgradeTool}
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

    Action(String description) {
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
    action.perform(getInjector());
  }

  public Injector getInjector() {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create();

    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      Modules.override(new LocationRuntimeModule().getDistributedModules()).with(new PrivateModule() {
        @Override
        protected void configure() {
          bind(NamespacedLocationFactory.class).to(SystemNamespacedLocationFactory.class).in(Scopes.SINGLETON);
          expose(NamespacedLocationFactory.class);
        }
      }),
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
        }
      });
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

    MetricsMigration(boolean keepOldMetricsData) {
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
                                                new HBaseTableUtilFactory(cConf).get());
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

  private interface MigrationAction {
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
      new InMemoryDatasetFramework(registryFactory);
    // TODO: this doesn't sound right. find out why its needed.
    datasetFramework.addModule(NamespaceId.SYSTEM.datasetModule("table"), new HBaseTableModule());
    datasetFramework.addModule(NamespaceId.SYSTEM.datasetModule("metricsTable"),
                               new HBaseMetricsTableModule());
    datasetFramework.addModule(NamespaceId.SYSTEM.datasetModule("core"), new CoreDatasetsModule());
    datasetFramework.addModule(NamespaceId.SYSTEM.datasetModule("fileSet"), new FileSetModule());
    return datasetFramework;
  }

  private static final class SystemNamespacedLocationFactory implements NamespacedLocationFactory {

    private final LocationFactory locationFactory;
    private final String namespaceDir;

    @Inject
    SystemNamespacedLocationFactory(CConfiguration cConf, LocationFactory locationFactory) {
      this.namespaceDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
      this.locationFactory = locationFactory;
    }

    @Override
    public Location get(Id.Namespace namespaceId) throws IOException {
      return get(namespaceId, null);
    }

    @Override
    public Location get(NamespaceMeta namespaceMeta) throws IOException {
      return get(namespaceMeta.getNamespaceId().toId(), null);
    }

    @Override
    public Location get(Id.Namespace namespaceId, @Nullable String subPath) throws IOException {
      // Only allow operates on system namespace
      if (!Id.Namespace.SYSTEM.equals(namespaceId)) {
        throw new IllegalArgumentException("Location operation on namespace " + namespaceId +
                                             " is not allowed. Only SYSTEM namespace is supported.");
      }

      Location namespaceLocation = locationFactory.create(namespaceDir).append(namespaceId.getId());
      if (subPath != null) {
        namespaceLocation = namespaceLocation.append(subPath);
      }
      return namespaceLocation;
    }

    @Override
    public Location getBaseLocation() throws IOException {
      return locationFactory.create("/");
    }
  }
}
