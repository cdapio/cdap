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

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.gateway.handlers.preview.PreviewHttpHandler;
import io.cdap.cdap.gateway.handlers.preview.PreviewHttpHandlerInternal;
import io.cdap.cdap.internal.app.preview.DefaultPreviewManager;
import io.cdap.cdap.internal.app.preview.DefaultPreviewRequestQueue;
import io.cdap.cdap.internal.app.preview.DistributedPreviewManager;
import io.cdap.cdap.internal.app.preview.DistributedPreviewRunStopper;
import io.cdap.cdap.internal.app.preview.PreviewDataCleanupService;
import io.cdap.cdap.internal.app.preview.PreviewRunStopper;
import io.cdap.cdap.internal.app.store.preview.DefaultPreviewStore;
import io.cdap.http.HttpHandler;

/**
 * Provides bindings for {@link PreviewManager}.
 */
public class PreviewManagerModule extends PrivateModule {

  private final boolean distributedRunner;

  public PreviewManagerModule(boolean distributedRunner) {
    this.distributedRunner = distributedRunner;
  }

  @Override
  protected void configure() {
    bind(DatasetDefinitionRegistryFactory.class)
      .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

    bind(DatasetFramework.class)
      .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
      .to(RemoteDatasetFramework.class);

    bind(PreviewStore.class).to(DefaultPreviewStore.class).in(Scopes.SINGLETON);
    bind(PreviewRequestQueue.class).to(DefaultPreviewRequestQueue.class).in(Scopes.SINGLETON);
    expose(PreviewRequestQueue.class);

    bind(PreviewDataCleanupService.class).in(Scopes.SINGLETON);

    if (distributedRunner) {
      bind(PreviewManager.class).to(DistributedPreviewManager.class).in(Scopes.SINGLETON);
      bind(PreviewRunStopper.class).to(DistributedPreviewRunStopper.class).in(Scopes.SINGLETON);
    } else {
      bind(PreviewManager.class).to(DefaultPreviewManager.class).in(Scopes.SINGLETON);
    }

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(PreviewHttpHandler.class);
    handlerBinder.addBinding().to(PreviewHttpHandlerInternal.class);
    CommonHandlers.add(handlerBinder);

    bind(PreviewHttpServer.class);
    expose(PreviewHttpServer.class);
  }
}
