/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
package co.cask.cdap.metrics.guice;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorService;
import co.cask.cdap.metrics.process.MessagingMetricsProcessorServiceFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.LocalMetricsDatasetFactory;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import com.google.inject.Module;
import com.google.inject.PrivateBinder;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

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
                  .implement(MessagingMetricsProcessorService.class, MessagingMetricsProcessorService.class)
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
    binder.bind(MetricDatasetFactory.class).to(LocalMetricsDatasetFactory.class).in(Scopes.SINGLETON);
    binder.bind(MetricStore.class).to(DefaultMetricStore.class);
    binder.expose(MetricStore.class);

    binder.bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
    binder.expose(MetricsCollectionService.class);
  }
}
