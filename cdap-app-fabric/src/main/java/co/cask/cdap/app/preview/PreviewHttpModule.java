/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.gateway.handlers.preview.PreviewHttpHandler;
import co.cask.cdap.internal.app.preview.DefaultPreviewManager;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.LogReader;
import co.cask.http.HttpHandler;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Provides bindings required create the {@link PreviewHttpHandler}.
 */
public class PreviewHttpModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return getStandaloneModules();
  }

  @Override
  public Module getStandaloneModules() {
    return new InternalPreviewHttpModule();
  }

  @Override
  public Module getDistributedModules() {
    return new InternalPreviewHttpModule() {
      @Override
      protected void addAdditionalBindings() {
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                          Names.named(Constants.Service.PREVIEW_HTTP));
        handlerBinder.addBinding().to(PreviewHttpHandler.class);
        CommonHandlers.add(handlerBinder);

        bind(LogReader.class).to(FileLogReader.class);
        expose(LogReader.class);
      }
    };
  }

  private class InternalPreviewHttpModule extends PrivateModule {
    @Override
    protected void configure() {
      bind(DatasetDefinitionRegistryFactory.class)
        .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

      bind(DatasetFramework.class)
        .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
        .to(RemoteDatasetFramework.class);
      bind(PreviewManager.class).to(DefaultPreviewManager.class).in(Scopes.SINGLETON);
      expose(PreviewManager.class);
      addAdditionalBindings();
    }

    protected void addAdditionalBindings() {};
  }
}
