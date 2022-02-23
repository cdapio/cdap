/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider.Mode;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.namespace.RemoteNamespaceQueryClient;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactoryProvider;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.RemoteConfigurator;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

public class DispatchTaskModule extends AbstractModule {

  private static final Key<TwillRunnerService> TWILL_RUNNER_SERVICE_KEY =
      Key.get(TwillRunnerService.class, Constants.AppFabric.RemoteExecution.class);

  @Override
  protected void configure() {
    // MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
    // bind(DiscoveryService.class)
    //     .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
    // bind(DiscoveryServiceClient.class)
    //     .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));

    // Bind ProgramRunner
    // MapBinder<ProgramType, ProgramRunner> runnerFactoryBinder =
    //     MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
    bind(ProgramStateWriter.class).to(NoOpProgramStateWriter.class);
    bind(ProgramRuntimeProvider.Mode.class).toInstance(Mode.DISTRIBUTED);
    bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);

    bind(PluginFinder.class).to(RemoteWorkerPluginFinder.class);
    bind(UGIProvider.class).to(CurrentUGIProvider.class);

    bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReader.class).in(Scopes.SINGLETON);
    bind(NamespaceQueryAdmin.class).to(RemoteNamespaceQueryClient.class);
    bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
    bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Singleton.class);

    bind(ArtifactRepository.class).to(RemoteArtifactRepository.class);

    install(
        new FactoryModuleBuilder()
            .implement(Configurator.class, InMemoryConfigurator.class)
            .build(Key.get(ConfiguratorFactory.class, Names.named("local")))
    );
    install(
        new FactoryModuleBuilder()
            .implement(Configurator.class, RemoteConfigurator.class)
            .build(Key.get(ConfiguratorFactory.class, Names.named("remote")))
    );

    bind(ConfiguratorFactory.class).toProvider(ConfiguratorFactoryProvider.class);

    // RemoteExecutionProgramRunnerModule

    bind(TWILL_RUNNER_SERVICE_KEY).to(RemoteExecutionTwillRunnerService.class).in(Scopes.SINGLETON);
    bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);

    // Bind ProgramRunnerFactory and expose it with the RemoteExecution annotation
    Key<ProgramRunnerFactory> programRunnerFactoryKey = Key.get(ProgramRunnerFactory.class,
        Constants.AppFabric.RemoteExecution.class);
    // ProgramRunnerFactory should be in distributed mode
    bind(programRunnerFactoryKey).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);

    // The following are bindings are for ProgramRunners. They are private to this module and only
    // available to the remote execution ProgramRunnerFactory exposed.

    // No need to publish program state for remote execution runs since they will publish states and get
    // collected back via the runtime monitoring
    bindConstant().annotatedWith(Names.named(DefaultProgramRunnerFactory.PUBLISH_PROGRAM_STATE)).to(false);
    // TwillRunner used by the ProgramRunner is the remote execution one
    bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class).to(TWILL_RUNNER_SERVICE_KEY);
    // ProgramRunnerFactory used by ProgramRunner is the remote execution one.
    bind(ProgramRunnerFactory.class)
        .annotatedWith(Constants.AppFabric.ProgramRunner.class)
        .to(programRunnerFactoryKey);

    // A private Map binding of ProgramRunner for ProgramRunnerFactory to use
    MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
        binder(), ProgramType.class, ProgramRunner.class);

    defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);
    defaultProgramRunnerBinder.addBinding(ProgramType.WORKFLOW).to(DistributedWorkflowProgramRunner.class);
    defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
  }
}
