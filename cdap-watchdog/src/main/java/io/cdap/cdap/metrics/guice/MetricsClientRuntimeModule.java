/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
package io.cdap.cdap.metrics.guice;

import com.google.inject.Module;
import com.google.inject.PrivateBinder;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.metrics.collect.LocalMetricsCollectionService;
import io.cdap.cdap.metrics.process.DirectMetricsSystemClient;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorManagerService;
import io.cdap.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import io.cdap.cdap.metrics.store.MetricsCleanUpService;

/**
 * A {@link RuntimeModule} that defines Guice modules for metrics collection in different runtime mode.
 */
public final class MetricsClientRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        addLocalBindings(binder());
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        addLocalBindings(binder());

        // Bind a processor for consuming metrics from TMS and write to MetricsStore
        bind(Integer.class).annotatedWith(Names.named(Constants.Metrics.TWILL_INSTANCE_ID)).toInstance(0);
        install(new FactoryModuleBuilder()
                  .implement(MessagingMetricsProcessorManagerService.class,
                             MessagingMetricsProcessorManagerService.class)
                  .build(MessagingMetricsProcessorServiceFactory.class));

        // We have to expose the factory in order for the optional injection in the LocalMetricsCollectionService
        // works. This is because FactoryModuleBuilder uses child injector and see
        // https://github.com/google/guice/issues/847 for the quirky behavior about optional inject and child injector
        expose(MessagingMetricsProcessorServiceFactory.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new DistributedMetricsClientModule();
  }

  /**
   * Adds bindings for collecting and writing metrics locally.
   */
  private void addLocalBindings(PrivateBinder binder) {
    // Install the MetricsStoreModule as private bindings and expose the MetricStore.
    // Both LocalMetricsCollectionService and the AppFabricService needs it
    binder.install(new MetricsStoreModule());
    binder.expose(MetricStore.class);
    binder.expose(MetricsCleanUpService.class);

    binder.bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
    binder.expose(MetricsCollectionService.class);

    // In local mode, we operates on the MetricsStore directly
    binder.bind(MetricsSystemClient.class).to(DirectMetricsSystemClient.class);
    binder.expose(MetricsSystemClient.class);
  }
}
