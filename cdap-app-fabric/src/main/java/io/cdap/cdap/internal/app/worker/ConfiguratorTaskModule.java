/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.namespace.RemoteNamespaceQueryClient;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * ConfiguratorTaskModule specifies the binding for a {@link io.cdap.cdap.internal.app.worker.ConfiguratorTask}
 */
public class ConfiguratorTaskModule extends AbstractModule {

  @Override
  protected void configure() {
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
    bind(DiscoveryService.class)
      .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
    bind(DiscoveryServiceClient.class)
      .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));

    // Bind ProgramRunner
    MapBinder<ProgramType, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
    bind(ProgramStateWriter.class).to(NoOpProgramStateWriter.class);
    bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.LOCAL);
    bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);

    bind(PluginFinder.class).to(RemoteWorkerPluginFinder.class);
    bind(UGIProvider.class).to(CurrentUGIProvider.class);

    bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReader.class).in(Scopes.SINGLETON);
    bind(NamespaceQueryAdmin.class).to(RemoteNamespaceQueryClient.class);
    bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);

    bind(ArtifactRepository.class).to(RemoteArtifactRepository.class);
  }
}

