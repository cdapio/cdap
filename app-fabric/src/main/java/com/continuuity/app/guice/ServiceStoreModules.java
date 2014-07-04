package com.continuuity.app.guice;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.app.store.ServiceStore;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.lib.kv.HBaseKVTableDefinition;
import com.continuuity.data2.dataset2.lib.kv.InMemoryKVTableDefinition;
import com.continuuity.data2.dataset2.lib.kv.LevelDBKVTableDefinition;
import com.continuuity.gateway.handlers.DatasetServiceStore;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

/**
 * ServiceStore Guice Modules.
 */
public class ServiceStoreModules {

  public Module getInMemoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));

        bind(new TypeLiteral<DatasetModule>() { }).annotatedWith(Names.named("serviceModule"))
          .toInstance(new InMemoryKVTableDefinition.Module());
        bind(ServiceStore.class).to(DatasetServiceStore.class).in(Scopes.SINGLETON);
        expose(ServiceStore.class);
      }
    };
  }

  public Module getSingleNodeModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));

        bind(new TypeLiteral<DatasetModule>() { }).annotatedWith(Names.named("serviceModule"))
          .toInstance(new LevelDBKVTableDefinition.Module());
        bind(ServiceStore.class).to(DatasetServiceStore.class).in(Scopes.SINGLETON);
        expose(ServiceStore.class);
      }
    };
  }

  public Module getDistributedModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));

        bind(new TypeLiteral<DatasetModule>() { }).annotatedWith(Names.named("serviceModule"))
          .toInstance(new HBaseKVTableDefinition.Module());
        bind(ServiceStore.class).to(DatasetServiceStore.class).in(Scopes.SINGLETON);
        expose(ServiceStore.class);
      }
    };
  }
}
