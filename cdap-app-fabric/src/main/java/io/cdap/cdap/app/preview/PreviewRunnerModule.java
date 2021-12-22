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
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Names;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.client.MockExploreClient;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
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
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.LocalArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.worker.RemoteWorkerPluginFinder;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.capability.CapabilityStatusStore;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.DefaultMetadataAdmin;
import io.cdap.cdap.metadata.LocalPreferencesFetcherInternal;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metadata.RemotePreferencesFetcherInternal;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.scheduler.NoOpScheduler;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.DefaultUGIProvider;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.cdap.store.DefaultOwnerStore;

/**
 * Provides bindings required to create injector for running preview.
 */
public class PreviewRunnerModule extends PrivateModule {
  private final CConfiguration cConf;
  private final ArtifactStore artifactStore;
  private final AccessControllerInstantiator accessControllerInstantiator;
  private final AccessEnforcer accessEnforcer;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private final PermissionManager permissionManager;
  private final PreferencesService preferencesService;
  private final ProgramRuntimeProviderLoader programRuntimeProviderLoader;
    private final MessagingService messagingService;

  @Inject
  PreviewRunnerModule(CConfiguration cConf,
                      ArtifactStore artifactStore,
                      AccessControllerInstantiator accessControllerInstantiator,
                      AccessEnforcer accessEnforcer,
                      ContextAccessEnforcer contextAccessEnforcer,
                      PermissionManager permissionManager, PreferencesService preferencesService,
                      ProgramRuntimeProviderLoader programRuntimeProviderLoader,
                      MessagingService messagingService) {
    this.cConf = cConf;
    this.artifactStore = artifactStore;
    this.accessControllerInstantiator = accessControllerInstantiator;
    this.accessEnforcer = accessEnforcer;
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.permissionManager = permissionManager;
    this.preferencesService = preferencesService;
    this.programRuntimeProviderLoader = programRuntimeProviderLoader;
    this.messagingService = messagingService;
  }

  @Override
  protected void configure() {
    Boolean artifactLocalizerEnabled = cConf.getBoolean(Constants.Preview.ARTIFACT_LOCALIZER_ENABLED, false);

    if (artifactLocalizerEnabled) {
      bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReader.class);
      bind(ArtifactRepository.class).to(RemoteArtifactRepository.class);
      expose(ArtifactRepository.class);

      bind(ArtifactRepository.class)
        .annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO))
        .to(RemoteArtifactRepository.class)
        .in(Scopes.SINGLETON);
      expose(ArtifactRepository.class).annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO));

      bind(PluginFinder.class).to(RemoteWorkerPluginFinder.class);
      expose(PluginFinder.class);

      bind(PreferencesFetcher.class).to(RemotePreferencesFetcherInternal.class);
      expose(PreferencesFetcher.class);

      OptionalBinder.newOptionalBinder(binder(), ArtifactLocalizerClient.class);
    } else {
      bind(ArtifactRepositoryReader.class).to(LocalArtifactRepositoryReader.class);
      bind(ArtifactRepository.class).to(DefaultArtifactRepository.class);
      expose(ArtifactRepository.class);

      bind(ArtifactRepository.class)
        .annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO))
        .to(DefaultArtifactRepository.class)
        .in(Scopes.SINGLETON);
      expose(ArtifactRepository.class).annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO));

      bind(PluginFinder.class).to(LocalPluginFinder.class);
      expose(PluginFinder.class);

      bind(PreferencesFetcher.class).to(LocalPreferencesFetcherInternal.class);
      expose(PreferencesFetcher.class);
    }

    bind(ArtifactStore.class).toInstance(artifactStore);
    expose(ArtifactStore.class);

    bind(MessagingService.class)
      .annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS))
      .toInstance(messagingService);
    expose(MessagingService.class).annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS));

    bind(AccessEnforcer.class).toInstance(accessEnforcer);
    expose(AccessEnforcer.class);
    bind(ContextAccessEnforcer.class).toInstance(contextAccessEnforcer);
    expose(ContextAccessEnforcer.class);
    bind(AccessControllerInstantiator.class).toInstance(accessControllerInstantiator);
    expose(AccessControllerInstantiator.class);
    bind(PermissionManager.class).toInstance(permissionManager);
    expose(PermissionManager.class);
    bind(PreferencesService.class).toInstance(preferencesService);
    // bind explore client to mock.
    bind(ExploreClient.class).to(MockExploreClient.class);
    expose(ExploreClient.class);
    bind(ProgramRuntimeProviderLoader.class).toInstance(programRuntimeProviderLoader);
    expose(ProgramRuntimeProviderLoader.class);
    bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);

    bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

    install(
      new FactoryModuleBuilder()
        .implement(Configurator.class, InMemoryConfigurator.class)
        .build(ConfiguratorFactory.class)
    );

    install(
      new FactoryModuleBuilder()
        .implement(new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() { },
                   new TypeLiteral<PreviewApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() { })
        .build(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() { })
    );

    bind(Store.class).to(DefaultStore.class);
    bind(SecretStore.class).to(DefaultSecretStore.class).in(Scopes.SINGLETON);

    bind(UGIProvider.class).to(DefaultUGIProvider.class);
    expose(UGIProvider.class);

    bind(WorkflowStateWriter.class).to(BasicWorkflowStateWriter.class);
    expose(WorkflowStateWriter.class);

    // we don't delete namespaces in preview as we just delete preview directory when its done
    bind(NamespaceResourceDeleter.class).to(NoopNamespaceResourceDeleter.class).in(Scopes.SINGLETON);
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
