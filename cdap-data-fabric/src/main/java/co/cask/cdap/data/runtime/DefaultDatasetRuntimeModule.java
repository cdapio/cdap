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

package co.cask.cdap.data.runtime;

import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.TimePartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.lib.table.ObjectMappedTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.data2.dataset2.module.lib.leveldb.LevelDBMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.leveldb.LevelDBTableModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;

/**
 * Provides guice bindings for {@link DatasetModule} that are by default available in the system.
 * The guice modules provided by this class are not intended to be used directly, but rather for providing
 * injections to {@link DatasetFramework}, hence for installing from other guice module.
 * This class is separated out so that combining different {@link DatasetFramework} and {@link DatasetModule} is
 * easier, especially for unit-test.
 */
public class DefaultDatasetRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(binder(), String.class, DatasetModule.class,
                                                                            Names.named("defaultDatasetModules"));
        // NOTE: order is important due to dependencies between modules
        mapBinder.addBinding("orderedTable-memory").toInstance(new InMemoryTableModule());
        mapBinder.addBinding("metricsTable-memory").toInstance(new InMemoryMetricsTableModule());
        bindDefaultModules(mapBinder);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(binder(), String.class, DatasetModule.class,
                                                                            Names.named("defaultDatasetModules"));
        // NOTE: order is important due to dependencies between modules
        mapBinder.addBinding("orderedTable-leveldb").toInstance(new LevelDBTableModule());
        mapBinder.addBinding("metricsTable-leveldb").toInstance(new LevelDBMetricsTableModule());
        bindDefaultModules(mapBinder);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(binder(), String.class, DatasetModule.class,
                                                                            Names.named("defaultDatasetModules"));

        // NOTE: order is important due to dependencies between modules
        mapBinder.addBinding("orderedTable-hbase").toInstance(new HBaseTableModule());
        mapBinder.addBinding("metricsTable-hbase").toInstance(new HBaseMetricsTableModule());
        bindDefaultModules(mapBinder);
      }
    };
  }

  /**
   * Add bindings for Dataset modules that are available by default
   */
  private void bindDefaultModules(MapBinder<String, DatasetModule> mapBinder) {
    mapBinder.addBinding("core").toInstance(new CoreDatasetsModule());
    mapBinder.addBinding("fileSet").toInstance(new FileSetModule());
    mapBinder.addBinding("timePartitionedFileSet").toInstance(new TimePartitionedFileSetModule());
    mapBinder.addBinding("partitionedFileSet").toInstance(new PartitionedFileSetModule());
    mapBinder.addBinding("objectMappedTable").toInstance(new ObjectMappedTableModule());

  }
}
