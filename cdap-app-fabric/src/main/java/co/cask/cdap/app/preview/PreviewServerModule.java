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

import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.store.PreviewStore;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.gateway.handlers.preview.PreviewHttpHandler;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceQueryAdmin;
import co.cask.cdap.internal.app.preview.DefaultPreviewManager;
import co.cask.cdap.internal.app.runtime.schedule.NoopScheduler;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.store.DefaultPreviewStore;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.pipeline.SynchronousPipelineFactory;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.http.HttpHandler;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

import java.util.Set;

/**
 * Preview server modules.
 */
public class PreviewServerModule extends PrivateModule {

  protected void configure() {
    bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

    install(
      new FactoryModuleBuilder()
        .implement(new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                   },
                   new TypeLiteral<PreviewApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                   })
        .build(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
        })
    );

    bind(Store.class).to(DefaultStore.class);
    expose(Store.class);

    bind(ProgramLifecycleService.class).in(Scopes.SINGLETON);
    expose(ProgramLifecycleService.class);
    bind(ApplicationLifecycleService.class).in(Scopes.SINGLETON);
    expose(ApplicationLifecycleService.class);

    bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
    expose(UGIProvider.class);

    bind(RuntimeStore.class).to(DefaultStore.class);
    expose(RuntimeStore.class);

    bind(NamespaceQueryAdmin.class).to(DefaultNamespaceQueryAdmin.class).in(Scopes.SINGLETON);
    expose(NamespaceQueryAdmin.class);

    bind(PreviewServer.class).in(Scopes.SINGLETON);
    expose(PreviewServer.class);

    bind(PreviewManager.class).to(DefaultPreviewManager.class).in(Scopes.SINGLETON);
    bind(PreviewStore.class).to(DefaultPreviewStore.class);
    expose(PreviewStore.class);

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
      binder(), HttpHandler.class, Names.named(Constants.Preview.HANDLERS_BINDING));

    CommonHandlers.add(handlerBinder);
    handlerBinder.addBinding().to(PreviewHttpHandler.class);
    expose(Key.get(new TypeLiteral<Set<HttpHandler>>() { },
                   Names.named(Constants.Preview.HANDLERS_BINDING)));

    bind(Scheduler.class).to(NoopScheduler.class);
  }
}
