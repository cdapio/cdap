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
package co.cask.cdap.metrics.guice;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.metrics.collect.AggregatedMetricsCollectionService;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.LocalMetricsDatasetFactory;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

import java.util.Iterator;

/**
 *
 */
public final class MetricsClientRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // Install the MetricsStoreModule as private bindings and expose the MetricStore.
        // Both LocalMetricsCollectionService and the AppFabricService needs it
        bind(MetricDatasetFactory.class).to(LocalMetricsDatasetFactory.class).in(Scopes.SINGLETON);
        bind(MetricStore.class).to(DefaultMetricStore.class);
        expose(MetricStore.class);

        bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
        expose(MetricsCollectionService.class);

      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // Install the MetricsStoreModule as private bindings and expose the MetricStore.
        // Both LocalMetricsCollectionService and the AppFabricService needs it
        bind(MetricDatasetFactory.class).to(LocalMetricsDatasetFactory.class).in(Scopes.SINGLETON);
        bind(MetricStore.class).to(DefaultMetricStore.class);
        expose(MetricStore.class);

        bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
        expose(MetricsCollectionService.class);
      }
    };
  }

  public Module getCollectionServiceBindingModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // skip the metric store metric dataset binding;
        // this module is used when injector also uses MetricsHandlerModule
        // MetricsHandler Module has the binding for the metric dataset and metric store
        bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
        expose(MetricsCollectionService.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new DistributedMetricsClientModule();
  }

  /**
   * Returns a module that bind MetricsCollectionService to a noop one.
   */
  public Module getNoopModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).toInstance(new AggregatedMetricsCollectionService() {
          @Override
          protected void publish(Iterator<MetricValues> metrics) throws Exception {
          }
        });
      }
    };
  }
}
