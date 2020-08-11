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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.client.MockExploreClient;
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
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.preview.DefaultPreviewStore;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.DefaultMetadataAdmin;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.scheduler.NoOpScheduler;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.DefaultUGIProvider;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import io.cdap.cdap.store.DefaultOwnerStore;

/**
 * Provides bindings required to create injector for running preview.
 */
public class DefaultPreviewRunnerModule extends PrivateModule implements PreviewRunnerModule {
  private final ArtifactRepository artifactRepository;
  private final ArtifactStore artifactStore;
  private final AuthorizerInstantiator authorizerInstantiator;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final PrivilegesManager privilegesManager;
  private final PreferencesService preferencesService;
  private final ProgramRuntimeProviderLoader programRuntimeProviderLoader;
  private final MessagingService messagingService;

  @VisibleForTesting
  @Inject
  public DefaultPreviewRunnerModule(ArtifactRepository artifactRepository, ArtifactStore artifactStore,
                                    AuthorizerInstantiator authorizerInstantiator,
                                    AuthorizationEnforcer authorizationEnforcer,
                                    PrivilegesManager privilegesManager, PreferencesService preferencesService,
                                    ProgramRuntimeProviderLoader programRuntimeProviderLoader,
                                    MessagingService messagingService) {
    this.artifactRepository = artifactRepository;
    this.artifactStore = artifactStore;
    this.authorizerInstantiator = authorizerInstantiator;
    this.authorizationEnforcer = authorizationEnforcer;
    this.privilegesManager = privilegesManager;
    this.preferencesService = preferencesService;
    this.programRuntimeProviderLoader = programRuntimeProviderLoader;
    this.messagingService = messagingService;
  }

  @Override
  protected void configure() {
    bind(ArtifactRepository.class).toInstance(artifactRepository);
    expose(ArtifactRepository.class);

    bind(ArtifactRepository.class)
      .annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO))
      .to(DefaultArtifactRepository.class)
      .in(Scopes.SINGLETON);
    expose(ArtifactRepository.class)
      .annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO));

    bind(ArtifactStore.class).toInstance(artifactStore);
    expose(ArtifactStore.class);

    bind(MessagingService.class)
      .annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS))
      .toInstance(messagingService);
    expose(MessagingService.class).annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS));

    bind(AuthorizerInstantiator.class).toInstance(authorizerInstantiator);
    expose(AuthorizerInstantiator.class);
    bind(AuthorizationEnforcer.class).toInstance(authorizationEnforcer);
    expose(AuthorizationEnforcer.class);
    bind(PrivilegesManager.class).toInstance(privilegesManager);
    expose(PrivilegesManager.class);
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
  }

  /**
   * Binds an implementation for {@link PreviewRunner}.
   */
  protected void bindPreviewRunner(Binder binder) {
    binder.bind(PreviewRunner.class).to(DefaultPreviewRunner.class).in(Scopes.SINGLETON);
  }
}
