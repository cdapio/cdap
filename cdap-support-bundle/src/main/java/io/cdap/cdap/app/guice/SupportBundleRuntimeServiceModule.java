/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.app.services.SupportBundleService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.AuthorizationArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.LocalArtifactRepositoryReader;
import io.cdap.cdap.internal.app.services.SupportBundleInternalService;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.logging.read.FileLogReader;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.logging.service.LogQueryService;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.DefaultUGIProvider;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.support.handlers.SupportBundleHttpHandler;
import io.cdap.cdap.support.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import io.cdap.http.HttpHandler;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * SupportBundle Service Guice Module.
 */
public final class SupportBundleRuntimeServiceModule extends RuntimeModule {

  public static final String NOAUTH_ARTIFACT_REPO = "noAuthArtifactRepo";

  @Override
  public Module getInMemoryModules() {
    return new SupportBundleServiceModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new SupportBundleServiceModule();
  }

  /**
   * Used by program containers and system services (viz explore service, stream service) that need
   * to enforce authorization in distributed mode.
   */
  @Override
  public Module getDistributedModules() {
    return Modules.combine(new SupportBundleServiceModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(Store.class).to(DefaultStore.class);
                               bind(LogReader.class).to(FileLogReader.class);
                               bind(UGIProvider.class).to(DefaultUGIProvider.class);
                               bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
                               install(new PrivateModule() {
                                 @Override
                                 protected void configure() {
                                   /* ArtifactRepositoryReader is required by DefaultArtifactRepository.
                                    Keep ArtifactRepositoryReader private
                                    to minimize the scope of the binding visibility.
                                    */
                                   bind(ArtifactRepositoryReader.class).to(
                                       LocalArtifactRepositoryReader.class).in(Scopes.SINGLETON);

                                   bind(ArtifactRepository.class)
                                       .annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO))
                                       .to(DefaultArtifactRepository.class)
                                       .in(Scopes.SINGLETON);
                                   expose(ArtifactRepository.class).annotatedWith(
                                       Names.named(NOAUTH_ARTIFACT_REPO));

                                   bind(ArtifactRepository.class).to(
                                       AuthorizationArtifactRepository.class).in(Scopes.SINGLETON);
                                   expose(ArtifactRepository.class);
                                 }
                               });
                             }
                           });
  }

  /**
   * Guice module for AppFabricServer. Requires data-fabric related bindings being available.
   */
  private static final class SupportBundleServiceModule extends AbstractModule {

    @Override
    protected void configure() {
      Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
          binder(), HttpHandler.class, Names.named(Constants.SupportBundle.HANDLERS_NAME));
      CommonHandlers.add(handlerBinder);
      handlerBinder.addBinding().to(SupportBundleHttpHandler.class);
      bind(SupportBundleInternalService.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Inject
    public List<SupportBundleTaskFactory> supportBundleTaskFactoryList(
        SupportBundleSystemLogTaskFactory supportBundleSystemLogTaskFactory,
        SupportBundlePipelineInfoTaskFactory supportBundlePipelineInfoTaskFactory) {
      return Arrays.asList(supportBundleSystemLogTaskFactory, supportBundlePipelineInfoTaskFactory);
    }

    @Provides
    @Named(Constants.SupportBundle.SERVICE_BIND_ADDRESS)
    @SuppressWarnings("unused")
    public InetAddress providesHostname(CConfiguration cConf) {
      String address = cConf.get(SupportBundle.SERVICE_BIND_ADDRESS);
      return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
