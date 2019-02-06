/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package co.cask.cdap.data.runtime;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.external.ExternalDataset;
import co.cask.cdap.data2.dataset2.lib.external.ExternalDatasetModule;
import co.cask.cdap.data2.dataset2.lib.file.FileSetDataset;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.TimePartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.lib.table.CubeModule;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.ObjectMappedTableModule;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseMetricsTableDefinition;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableDefinition;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTableDefinition;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableDefinition;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBMetricsTableDefinition;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableDefinition;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.data2.dataset2.module.lib.leveldb.LevelDBMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.leveldb.LevelDBTableModule;
import co.cask.cdap.data2.metadata.dataset.MetadataDatasetModule;
import co.cask.cdap.data2.metadata.lineage.LineageDatasetModule;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageDatasetModule;
import co.cask.cdap.data2.registry.UsageDatasetModule;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;

/**
 * Provides guice bindings for {@link DatasetModule} that are by default available in the system.
 * The guice modules provided by this class are not intended to be used directly, but rather for providing
 * injections to {@link DatasetFramework}, hence for installing from other guice module.
 * This class is separated out so that combining different {@link DatasetFramework} and {@link DatasetModule} is
 * easier, especially for unit-test.
 */
public class SystemDatasetRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(
          binder(), String.class, DatasetModule.class, Constants.Dataset.Manager.DefaultDatasetModules.class);
        // NOTE: order is important due to dependencies between modules
        mapBinder.addBinding("orderedTable-memory").toInstance(new InMemoryTableModule());
        mapBinder.addBinding("metricsTable-memory").toInstance(new InMemoryMetricsTableModule());
        bindDefaultModules(mapBinder);

        bind(String.class)
          .annotatedWith(Names.named(Constants.Dataset.TABLE_TYPE)).toInstance("table");
        bind(DatasetDefinition.class)
          .annotatedWith(Names.named(Constants.Dataset.TABLE_TYPE)).to(InMemoryTableDefinition.class);

        // Direct binding for the Metrics table definition such that metrics system doesn't need to go through
        // dataset service to get metrics table.
        bind(new TypeLiteral<DatasetDefinition<MetricsTable, DatasetAdmin>>() { })
          .toInstance(new InMemoryMetricsTableDefinition(MetricsTable.class.getName()));
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(
          binder(), String.class, DatasetModule.class, Constants.Dataset.Manager.DefaultDatasetModules.class);
        // NOTE: order is important due to dependencies between modules
        mapBinder.addBinding("orderedTable-leveldb").toInstance(new LevelDBTableModule());
        mapBinder.addBinding("metricsTable-leveldb").toInstance(new LevelDBMetricsTableModule());
        bindDefaultModules(mapBinder);

        bind(String.class)
          .annotatedWith(Names.named(Constants.Dataset.TABLE_TYPE)).toInstance("table");
        bind(DatasetDefinition.class)
          .annotatedWith(Names.named(Constants.Dataset.TABLE_TYPE)).to(LevelDBTableDefinition.class);

        // Direct binding for the Metrics table definition such that metrics system doesn't need to go through
        // dataset service to get metrics table.
        bind(new TypeLiteral<DatasetDefinition<MetricsTable, DatasetAdmin>>() { })
          .toInstance(new LevelDBMetricsTableDefinition(MetricsTable.class.getName()));
      }
    };
  }

  private static final class OrderedTableModuleProvider implements Provider<DatasetModule> {
    private final CConfiguration cConf;

    @Inject
    private OrderedTableModuleProvider(CConfiguration cConf) {
      this.cConf = cConf;
    }

    @Override
    public DatasetModule get() {
      String moduleName = cConf.get(Constants.Dataset.Extensions.DISTMODE_TABLE);

      if (moduleName != null) {
        try {
          return (DatasetModule) Class.forName(moduleName.trim()).newInstance();
        } catch (ClassCastException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
          // Guice frowns on throwing exceptions from Providers, but if necessary use RuntimeException
          throw new RuntimeException("Unable to obtain distributed table module extension class", ex);
        }
      } else {
        return new HBaseTableModule();
      }
    }
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(
          binder(), String.class, DatasetModule.class, Constants.Dataset.Manager.DefaultDatasetModules.class);

        // NOTE: order is important due to dependencies between modules
        mapBinder.addBinding("orderedTable-hbase").toProvider(OrderedTableModuleProvider.class).in(Singleton.class);
        mapBinder.addBinding("metricsTable-hbase").toInstance(new HBaseMetricsTableModule());
        bindDefaultModules(mapBinder);

        bind(String.class)
          .annotatedWith(Names.named(Constants.Dataset.TABLE_TYPE)).toInstance("table");
        bind(DatasetDefinition.class)
          .annotatedWith(Names.named(Constants.Dataset.TABLE_TYPE)).to(HBaseTableDefinition.class);

        // Direct binding for the Metrics table definition such that metrics system doesn't need to go through
        // dataset service to get metrics table.
        bind(new TypeLiteral<DatasetDefinition<MetricsTable, DatasetAdmin>>() { })
          .toInstance(new HBaseMetricsTableDefinition(MetricsTable.class.getName()));
      }
    };
  }

  /**
   * Add bindings for Dataset modules that are available by default
   */
  private void bindDefaultModules(MapBinder<String, DatasetModule> mapBinder) {
    mapBinder.addBinding("core").toInstance(new CoreDatasetsModule());
    mapBinder.addBinding(FileSetDataset.TYPE).toInstance(new FileSetModule());
    mapBinder.addBinding(TimePartitionedFileSet.TYPE).toInstance(new TimePartitionedFileSetModule());
    mapBinder.addBinding(PartitionedFileSet.TYPE).toInstance(new PartitionedFileSetModule());
    mapBinder.addBinding(ObjectMappedTable.TYPE).toInstance(new ObjectMappedTableModule());
    mapBinder.addBinding(Cube.TYPE).toInstance(new CubeModule());
    mapBinder.addBinding("usage").toInstance(new UsageDatasetModule());
    mapBinder.addBinding("metadata").toInstance(new MetadataDatasetModule());
    mapBinder.addBinding("lineage").toInstance(new LineageDatasetModule());
    mapBinder.addBinding("fieldLineage").toInstance(new FieldLineageDatasetModule());
    mapBinder.addBinding(ExternalDataset.TYPE).toInstance(new ExternalDatasetModule());
  }
}
