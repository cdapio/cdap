/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.kv.HBaseKVTableDefinition;
import co.cask.cdap.data2.dataset2.lib.kv.InMemoryKVTableDefinition;
import co.cask.cdap.gateway.handlers.DatasetServiceStore;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import java.util.Map;

/**
 * ServiceStore Guice Modules.
 */
public class ServiceStoreModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new ServiceStoreModule(new InMemoryKVTableDefinition.Module());
  }

  @Override
  public Module getStandaloneModules() {
    return new ServiceStoreModule(new InMemoryKVTableDefinition.Module());
  }

  @Override
  public Module getDistributedModules() {
    return new ServiceStoreModule(new HBaseKVTableDefinition.Module());
  }

  private static final class ServiceStoreModule extends PrivateModule {

    private final DatasetModule tableModule;

    private ServiceStoreModule(DatasetModule tableModule) {
      this.tableModule = tableModule;
    }

    @Override
    protected void configure() {
      // The ServiceStore uses a special non-TX KV Table.
      MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(binder(), String.class, DatasetModule.class,
                                                                          Names.named("service.store.ds.modules"));
      mapBinder.addBinding("basicKVTable").toInstance(tableModule);

      // Need to use a provider here because there are cases which the binding for
      // DatasetDefinitionRegistryFactory is defined publicly, hence cannot create private binding here
      // Need to fix/reorganize all guice modules to make things work properly.
      bind(DatasetFramework.class)
        .annotatedWith(Names.named("local.ds.framework")).toProvider(DatasetFrameworkProvider.class);
      bind(ServiceStore.class).to(DatasetServiceStore.class).in(Scopes.SINGLETON);
      expose(ServiceStore.class);
    }
  }

  /**
   * A Guice provider for {@link DatasetFramework} with {@link InMemoryDatasetFramework} as implementation.
   */
  private static final class DatasetFrameworkProvider implements Provider<DatasetFramework> {

    private final Injector injector;
    private final CConfiguration cConf;
    private final Map<String, DatasetModule> datasetModules;

    @Inject
    private DatasetFrameworkProvider(Injector injector, CConfiguration cConf,
                                     @Named("service.store.ds.modules") Map<String, DatasetModule> datasetModules) {
      this.injector = injector;
      this.cConf = cConf;
      this.datasetModules = datasetModules;
    }

    @Override
    public DatasetFramework get() {
      DatasetDefinitionRegistryFactory registryFactory = new DatasetDefinitionRegistryFactory() {
        @Override
        public DatasetDefinitionRegistry create() {
          DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
          injector.injectMembers(registry);
          return registry;
        }
      };

      return new InMemoryDatasetFramework(registryFactory, datasetModules, cConf);
    }
  }
}
