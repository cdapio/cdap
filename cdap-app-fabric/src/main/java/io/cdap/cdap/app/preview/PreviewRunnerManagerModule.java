/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import com.google.inject.Exposed;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.internal.app.preview.DirectPreviewRequestFetcher;
import io.cdap.cdap.internal.app.preview.PreviewRequestFetcher;
import io.cdap.cdap.internal.app.preview.PreviewRequestPollerInfoProvider;
import io.cdap.cdap.internal.app.preview.PreviewRunStopper;
import io.cdap.cdap.internal.app.preview.PreviewRunnerService;
import io.cdap.cdap.internal.app.preview.RemotePreviewRequestFetcher;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Guice module to provide bindings for {@link PreviewRunnerManager} service.
 */
public class PreviewRunnerManagerModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return getStandaloneModules();
  }

  @Override
  public Module getStandaloneModules() {

    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistryFactory.class)
          .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
          .to(RemoteDatasetFramework.class);
        bind(PreviewRunnerModule.class).to(DefaultPreviewRunnerModule.class);

        bind(DefaultPreviewRunnerManager.class).in(Scopes.SINGLETON);
        bind(PreviewRunStopper.class).to(DefaultPreviewRunnerManager.class);
        expose(PreviewRunStopper.class);
        bind(PreviewRunnerManager.class).to(DefaultPreviewRunnerManager.class);
        expose(PreviewRunnerManager.class);

        install(new FactoryModuleBuilder()
                  .implement(PreviewRunnerService.class, PreviewRunnerService.class)
                  .build(PreviewRunnerServiceFactory.class));
      }

      @Provides
      @Singleton
      @Exposed
      PreviewRequestFetcher getPreviewRequestQueueFetcher(PreviewRequestQueue previewRequestQueue) {
        return new DirectPreviewRequestFetcher(previewRequestQueue);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistryFactory.class)
          .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

        bind(DatasetFramework.class)
          .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
          .to(RemoteDatasetFramework.class);
        bind(PreviewRunnerModule.class).to(DefaultPreviewRunnerModule.class);

        bind(DefaultPreviewRunnerManager.class).in(Scopes.SINGLETON);
        bind(PreviewRunnerManager.class).to(DefaultPreviewRunnerManager.class);
        expose(PreviewRunnerManager.class);

        install(new FactoryModuleBuilder()
                  .implement(PreviewRunnerService.class, PreviewRunnerService.class)
                  .build(PreviewRunnerServiceFactory.class));
      }

      @Provides
      @Singleton
      @Exposed
      PreviewRequestFetcher getPreviewRequestQueueFetcher(
        DiscoveryServiceClient discoveryServiceClient,
        PreviewRequestPollerInfoProvider previewRequestPollerInfoProvider) {
        return new RemotePreviewRequestFetcher(discoveryServiceClient, previewRequestPollerInfoProvider.get());
      }
    };
  }
}
