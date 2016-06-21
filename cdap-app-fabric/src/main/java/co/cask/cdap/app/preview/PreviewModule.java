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
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.internal.app.deploy.LocalApplicationManager;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.pipeline.SynchronousPipelineFactory;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.http.HttpHandler;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Metrics http handlers.
 */
public class PreviewModule {

  public Module getPreviewModules(ArtifactRepository artifactRepository) {
    return Modules.combine(new PreviewServiceModule(artifactRepository),
                           new ConfigStoreModule().getStandaloneModule());
  }

  private static final class PreviewServiceModule extends AbstractModule {
    ArtifactRepository artifactRepository;

    PreviewServiceModule(ArtifactRepository artifactRepository) {
      this.artifactRepository = artifactRepository;
    }
    @Override
    protected void configure() {
      bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

      install(
        new FactoryModuleBuilder()
          .implement(new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     },
                     new TypeLiteral<LocalApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     })
          .build(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
          })
      );

      bind(Store.class).to(DefaultStore.class);
      bind(PreviewServer.class).in(Scopes.SINGLETON);
      bind(ArtifactRepository.class).toInstance(artifactRepository);
      bind(ArtifactStore.class).in(Scopes.SINGLETON);
      bind(ProgramLifecycleService.class).in(Scopes.SINGLETON);
      bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);

      Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
        binder(), HttpHandler.class, Names.named(Constants.Preview.HANDLERS_BINDING));

      CommonHandlers.add(handlerBinder);
      handlerBinder.addBinding().to(PreviewHttpHandler.class);
    }

    @Provides
    @Named(Constants.AppFabric.SERVER_ADDRESS)
    @SuppressWarnings("unused")
    public InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.AppFabric.SERVER_ADDRESS),
                              new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}