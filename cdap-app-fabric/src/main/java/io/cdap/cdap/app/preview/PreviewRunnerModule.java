/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.InMemoryProgramRunDispatcher;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.NamespaceResourceDeleter;
import io.cdap.cdap.internal.app.namespace.NoopNamespaceResourceDeleter;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.preview.DefaultDataTracerFactory;
import io.cdap.cdap.internal.app.preview.DefaultPreviewRunner;
import io.cdap.cdap.internal.app.preview.MessagingPreviewDataPublisher;
import io.cdap.cdap.internal.app.preview.PreviewPluginFinder;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.common.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReaderWithLocalization;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryWithLocalization;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.capability.CapabilityStatusStore;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.DefaultMetadataAdmin;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metadata.RemotePreferencesFetcherInternal;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.scheduler.NoOpScheduler;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.DefaultUGIProvider;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.store.DefaultOwnerStore;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Provides bindings required to create injector for running preview.
 */
public class PreviewRunnerModule extends PrivateModule {

  private final CConfiguration cConf;
  private final AccessEnforcer accessEnforcer;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private final ProgramRuntimeProviderLoader programRuntimeProviderLoader;
  private final MessagingService messagingService;

  @Inject
  PreviewRunnerModule(CConfiguration cConf,
      AccessEnforcer accessEnforcer,
      ContextAccessEnforcer contextAccessEnforcer,
      ProgramRuntimeProviderLoader programRuntimeProviderLoader,
      MessagingService messagingService) {
    this.cConf = cConf;
    this.accessEnforcer = accessEnforcer;
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.programRuntimeProviderLoader = programRuntimeProviderLoader;
    this.messagingService = messagingService;
  }

  @Override
  protected void configure() {
    // Use remote implementation to fetch artifact metadata from AppFab.
    // Remote implementation internally uses artifact localizer to fetch and cache artifacts locally.
    bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReaderWithLocalization.class);
    bind(ArtifactRepository.class).to(RemoteArtifactRepositoryWithLocalization.class);
    expose(ArtifactRepository.class);
    bind(ArtifactRepository.class)
        .annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO))
        .to(RemoteArtifactRepositoryWithLocalization.class)
        .in(Scopes.SINGLETON);
    expose(ArtifactRepository.class).annotatedWith(
        Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO));

    // Use preview implementation to fetch plugin metadata from AppFab.
    // Preview implementation internally uses artifact localizer to fetch and cache artifacts locally.
    bind(PluginFinder.class).to(PreviewPluginFinder.class);
    expose(PluginFinder.class);

    // Read artifact locations from disk when using artifact localizer due to shared mounted read-only PD.
    install(new LocalLocationModule());
    expose(LocationFactory.class);

    // Use remote implementation to fetch preferences from AppFab.
    bind(PreferencesFetcher.class).to(RemotePreferencesFetcherInternal.class);
    expose(PreferencesFetcher.class);

    bind(MessagingService.class)
        .annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS))
        .toInstance(messagingService);
    expose(MessagingService.class).annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS));

    bind(AccessEnforcer.class).toInstance(accessEnforcer);
    expose(AccessEnforcer.class);
    bind(ContextAccessEnforcer.class).toInstance(contextAccessEnforcer);
    expose(ContextAccessEnforcer.class);
    bind(ProgramRuntimeProviderLoader.class).toInstance(programRuntimeProviderLoader);
    expose(ProgramRuntimeProviderLoader.class);
    bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);

    bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

    install(
        new FactoryModuleBuilder()
            .implement(Configurator.class, InMemoryConfigurator.class)
            .build(ConfiguratorFactory.class)
    );
    // expose this binding so program runner modules can use
    expose(ConfiguratorFactory.class);

    bind(InMemoryProgramRunDispatcher.class).in(Scopes.SINGLETON);
    expose(InMemoryProgramRunDispatcher.class);

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
    bind(SecretStore.class).to(DefaultSecretStore.class).in(Scopes.SINGLETON);

    bind(UGIProvider.class).to(DefaultUGIProvider.class);
    expose(UGIProvider.class);

    bind(WorkflowStateWriter.class).to(BasicWorkflowStateWriter.class);
    expose(WorkflowStateWriter.class);

    // we don't delete namespaces in preview as we just delete preview directory when its done
    bind(NamespaceResourceDeleter.class).to(NoopNamespaceResourceDeleter.class)
        .in(Scopes.SINGLETON);
    bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
    bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
    expose(NamespaceAdmin.class);
    expose(NamespaceQueryAdmin.class);

    bind(MetadataAdmin.class).to(DefaultMetadataAdmin.class);
    expose(MetadataAdmin.class);

    bindPreviewRunner(binder());
    expose(PreviewRunner.class);

    bind(Scheduler.class).to(NoOpScheduler.class);

    bind(DataTracerFactory.class).to(DefaultDataTracerFactory.class);
    expose(DataTracerFactory.class);

    bind(PreviewDataPublisher.class).to(MessagingPreviewDataPublisher.class);

    bind(OwnerStore.class).to(DefaultOwnerStore.class);
    expose(OwnerStore.class);
    bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
    expose(OwnerAdmin.class);

    bind(CapabilityReader.class).to(CapabilityStatusStore.class);
  }

  /**
   * Binds an implementation for {@link PreviewRunner}.
   */
  protected void bindPreviewRunner(Binder binder) {
    binder.bind(PreviewRunner.class).to(DefaultPreviewRunner.class).in(Scopes.SINGLETON);
  }
}
