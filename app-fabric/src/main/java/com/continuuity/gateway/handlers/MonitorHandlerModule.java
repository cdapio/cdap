package com.continuuity.gateway.handlers;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

/**
 *
 */
public class MonitorHandlerModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
              .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
              .build(DatasetDefinitionRegistryFactory.class));
  }
}
