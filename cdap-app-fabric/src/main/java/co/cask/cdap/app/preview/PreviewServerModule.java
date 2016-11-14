/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.preview;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.gateway.handlers.preview.PreviewHttpHandler;
import co.cask.http.HttpHandler;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Provides bindings required create the {@link PreviewServer}.
 */
public class PreviewServerModule extends PrivateModule {
  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
              .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
              .build(DatasetDefinitionRegistryFactory.class));
    bind(DatasetFramework.class)
      .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
      .to(RemoteDatasetFramework.class);
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
      binder(), HttpHandler.class, Names.named(Constants.Preview.HANDLERS_BINDING));
    CommonHandlers.add(handlerBinder);
    handlerBinder.addBinding().to(PreviewHttpHandler.class);
    bind(PreviewServer.class).in(Scopes.SINGLETON);
    expose(PreviewServer.class);
    bind(PreviewHttpHandler.class).in(Scopes.SINGLETON);
    expose(PreviewHttpHandler.class);
  }
}
