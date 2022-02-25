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

package io.cdap.cdap.internal.app.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.deploy.Dispatcher;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.RemoteExecutionProgramRunnerModule;
import io.cdap.cdap.app.guice.RemoteExecutionProgramRunnerModule.ProgramCompletionNotifierProvider;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactoryProvider;
import io.cdap.cdap.internal.app.deploy.DispatcherFactory;
import io.cdap.cdap.internal.app.deploy.DispatcherFactoryProvider;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.InMemoryDispatcher;
import io.cdap.cdap.internal.app.deploy.RemoteConfigurator;
import io.cdap.cdap.internal.app.deploy.RemoteDispatcher;
import io.cdap.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactManager;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.services.ProgramCompletionNotifier;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.security.auth.FileBasedKeyManager;
import io.cdap.cdap.security.auth.KeyManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.RemoteUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * The {@link TwillRunnable} for running {@link TaskWorkerService}.
 */
public class TaskWorkerTwillRunnable extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerTwillRunnable.class);

  private TaskWorkerService taskWorker;
  private LogAppenderInitializer logAppenderInitializer;
  private MetricsCollectionService metricsCollectionService;

  public TaskWorkerTwillRunnable(String cConfFileName, String hConfFileName, String sConfFileName) {
    super(ImmutableMap.of("cConf", cConfFileName, "hConf", hConfFileName, "sConf", sConfFileName));
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf, SConfiguration sConf) {
    List<Module> modules = new ArrayList<>();

    // CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    // modules.add(new AppFabricServiceRuntimeModule(cConf));
    modules.add(new TwillModule());
    modules.add(new ConfigModule(cConf, hConf, sConf));
    modules.add(new LocalLocationModule());
    modules.add(new IOModule());
    modules.add(new AuthenticationContextModules().getMasterWorkerModule());
    // modules.add(coreSecurityModule);
    modules.add(new CoreSecurityModule() {
      @Override
      protected void bindKeyManager(Binder binder) {
        LOG.debug("Going to bind KeyManager in Injector!!!");
        binder.bind(KeyManager.class).to(FileBasedKeyManager.class).in(Scopes.SINGLETON);
        expose(KeyManager.class);
      }
    });
    modules.add(new MessagingClientModule());
    // modules.add(new SystemAppModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    // modules.add(new RemoteExecutionProgramRunnerModule());
    // modules.add(new ProgramRunnerRuntimeModule().getDistributedModules());
    modules.add(new SecureStoreServerModule());
    modules.add(new ProvisionerModule());
    modules.add(new StorageModule());
    modules.add(new AuthorizationEnforcementModule().getDistributedModules());
    // modules.add(new NamespaceQueryAdminModule());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(Store.class).to(DefaultStore.class);
        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
        bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReader.class).in(Scopes.SINGLETON);
        bind(ArtifactRepository.class).to(RemoteArtifactRepository.class);
        bind(PluginFinder.class).to(LocalPluginFinder.class);
        bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class).in(Scopes.SINGLETON);
        bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
        install(new FactoryModuleBuilder()
            .implement(ArtifactManager.class, RemoteArtifactManager.class)
            .build(ArtifactManagerFactory.class));
        install(
            new FactoryModuleBuilder()
                .implement(Dispatcher.class, InMemoryDispatcher.class)
                .build(Key.get(DispatcherFactory.class, Names.named("local")))
        );
        install(
            new FactoryModuleBuilder()
                .implement(Dispatcher.class, RemoteDispatcher.class)
                .build(Key.get(DispatcherFactory.class, Names.named("remote")))
        );
        bind(DispatcherFactory.class).toProvider(DispatcherFactoryProvider.class);

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

        Key<TwillRunnerService> twillRunnerServiceKey =
            Key.get(TwillRunnerService.class, Constants.AppFabric.RemoteExecution.class);

        // Bind the TwillRunner for remote execution used in isolated cluster.
        // The binding is added in here instead of in TwillModule is because this module can be used
        // in standalone env as well and it doesn't require YARN.
        bind(twillRunnerServiceKey).to(RemoteExecutionTwillRunnerService.class).in(Scopes.SINGLETON);

        // Bind ProgramRunnerFactory and expose it with the RemoteExecution annotation
        Key<ProgramRunnerFactory> programRunnerFactoryKey = Key.get(ProgramRunnerFactory.class,
            Constants.AppFabric.RemoteExecution.class);
        // ProgramRunnerFactory should be in distributed mode
        bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.DISTRIBUTED);
        bind(programRunnerFactoryKey).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);

        // The following are bindings are for ProgramRunners. They are private to this module and only
        // available to the remote execution ProgramRunnerFactory exposed.

        // This set of program runners are for isolated mode
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
        // No need to publish program state for remote execution runs since they will publish states and get
        // collected back via the runtime monitoring
        bindConstant().annotatedWith(Names.named(DefaultProgramRunnerFactory.PUBLISH_PROGRAM_STATE)).to(false);
        // TwillRunner used by the ProgramRunner is the remote execution one
        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class).to(twillRunnerServiceKey);
        // ProgramRunnerFactory used by ProgramRunner is the remote execution one.
        bind(ProgramRunnerFactory.class)
            .annotatedWith(Constants.AppFabric.ProgramRunner.class)
            .to(programRunnerFactoryKey);

        // A private Map binding of ProgramRunner for ProgramRunnerFactory to use
        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
            binder(), ProgramType.class, ProgramRunner.class);

        defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(
            DistributedMapReduceProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKFLOW).to(
            DistributedWorkflowProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
        Multibinder<ProgramCompletionNotifier> multiBinder = Multibinder.newSetBinder(binder(),
            ProgramCompletionNotifier.class);
        multiBinder.addBinding().toProvider(ProgramCompletionNotifierProvider.class);

        // Bindings to get Provisioning Service working
        bind(SecretStore.class).to(DefaultSecretStore.class).in(Scopes.SINGLETON);
        bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class);
      }
    });

    // If MasterEnvironment is not available, assuming it is the old hadoop stack with ZK, Kafka
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();

    if (masterEnv == null) {
      modules.add(new ZKClientModule());
      modules.add(new ZKDiscoveryModule());
      modules.add(new KafkaClientModule());
      modules.add(new KafkaLogAppenderModule());
    } else {
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          bind(DiscoveryService.class)
              .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
          bind(DiscoveryServiceClient.class)
              .toProvider(
                  new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
        }
      });
      modules.add(new RemoteLogAppenderModule());

      // if (coreSecurityModule.requiresZKClient()) {
      //   modules.add(new ZKClientModule());
      // }
    }

    return Guice.createInjector(modules);
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);
    LOG.debug("Starting Task Worker doInit()");
    try {
      doInitialize(context);
    } catch (Exception e) {
      LOG.error("Encountered error while initializing TaskWorkerTwillRunnable", e);
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
    LOG.debug("Ending Task Worker doInit()");
  }

  @Override
  public void run() {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    taskWorker.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    LOG.debug("Starting task worker");
    taskWorker.start();

    try {
      Uninterruptibles.getUninterruptibly(future);
      LOG.debug("Task worker stopped");
    } catch (ExecutionException e) {
      LOG.warn("Task worker stopped with exception", e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping task worker");
    Optional.ofNullable(metricsCollectionService).map(MetricsCollectionService::stop);
    if (taskWorker != null) {
      taskWorker.stop();
    }
  }

  @Override
  public void destroy() {
    if (logAppenderInitializer != null) {
      logAppenderInitializer.close();
    }
  }

  private void doInitialize(TwillContext context) throws Exception {
    CConfiguration cConf = CConfiguration.create(new File(getArgument("cConf")).toURI().toURL());

    // Overwrite the app fabric temp directory with the task worker temp directory
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, cConf.get(Constants.TaskWorker.LOCAL_DATA_DIR));

    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(new File(getArgument("hConf")).toURI().toURL());

    SConfiguration sConf = SConfiguration.create(new File(getArgument("sConf")));

    Injector injector = createInjector(cConf, hConf, sConf);
    try {
      LOG.debug("KeyManager while binding {}", injector.getProvider(KeyManager.class).get().toString());
    } catch (Exception e) {
      LOG.error("Doesn't work this way", e);
    }

    // Initialize logging context
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();

    LoggingContext loggingContext = new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
        Constants.Logging.COMPONENT_NAME,
        TaskWorkerTwillApplication.NAME);
    LoggingContextAccessor.setLoggingContext(loggingContext);
    taskWorker = injector.getInstance(TaskWorkerService.class);
  }
}
