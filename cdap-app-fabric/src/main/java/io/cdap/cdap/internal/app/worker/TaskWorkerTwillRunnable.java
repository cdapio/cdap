/*
 * Copyright © 2021-2022 Cask Data, Inc.
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
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.RemoteTwillModule;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.tethering.TetheringClientSubscriberService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.operations.guice.OperationalStatsModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.FileBasedKeyManager;
import io.cdap.cdap.security.auth.KeyManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.FileBasedCoreSecurityModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
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
    // List<Module> modules = new ArrayList<>();

    // modules.add(new AppFabricServiceRuntimeModule(cConf).getDistributedModules());
    // modules.add(new ConfigModule(cConf, hConf, sConf));
    // modules.add(new LocalLocationModule());
    // modules.add(RemoteAuthenticatorModules.getDefaultModule());
    // CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    // modules.add(new AppFabricServiceRuntimeModule(cConf));
    // modules.add(new TwillModule());
    // modules.add(new ConfigModule(cConf, hConf, sConf));
    // modules.add(RemoteAuthenticatorModules.getDefaultModule());
    // modules.add(new LocalLocationModule());
    // modules.add(new IOModule());
    // modules.add(new AuthenticationContextModules().getMasterWorkerModule());
    // // modules.add(coreSecurityModule);
    // modules.add(new CoreSecurityModule() {
    //   @Override
    //   protected void bindKeyManager(Binder binder) {
    //     LOG.debug("Going to bind KeyManager in Injector!!!");
    //     binder.bind(KeyManager.class).to(FileBasedKeyManager.class).in(Scopes.SINGLETON);
    //     expose(KeyManager.class);
    //   }
    // });
    // modules.add(new MessagingClientModule());
    // // modules.add(new SystemAppModule());
    // modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    // // modules.add(new RemoteExecutionProgramRunnerModule());
    // // modules.add(new ProgramRunnerRuntimeModule().getDistributedModules());
    // modules.add(new SecureStoreServerModule());
    // modules.add(new ProvisionerModule());
    // modules.add(new StorageModule());
    // modules.add(new AuthorizationEnforcementModule().getDistributedModules());
    // modules.add(new AbstractModule() {
    //   @Override
    //   protected void configure() {
    //     bind(Store.class).to(DefaultStore.class);
    //     bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
    //     bind(ArtifactRepositoryReader.class).to(RemoteArtifactRepositoryReader.class).in(Scopes.SINGLETON);
    //     bind(ArtifactRepository.class).to(RemoteArtifactRepository.class);
    //     bind(PluginFinder.class).to(LocalPluginFinder.class);
    //     bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class).in(Scopes.SINGLETON);
    //     bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
    //     install(new FactoryModuleBuilder()
    //         .implement(ArtifactManager.class, RemoteArtifactManager.class)
    //         .build(ArtifactManagerFactory.class));
    //     install(
    //         new FactoryModuleBuilder()
    //             .implement(LaunchDispatcher.class, InMemoryLaunchDispatcher.class)
    //             .build(Key.get(DispatcherFactory.class, Names.named("local")))
    //     );
    //     install(
    //         new FactoryModuleBuilder()
    //             .implement(LaunchDispatcher.class, RemoteLaunchDispatcher.class)
    //             .build(Key.get(DispatcherFactory.class, Names.named("remote")))
    //     );
    //     bind(DispatcherFactory.class).toProvider(DispatcherFactoryProvider.class);
    //
    //     install(
    //         new FactoryModuleBuilder()
    //             .implement(Configurator.class, InMemoryConfigurator.class)
    //             .build(Key.get(ConfiguratorFactory.class, Names.named("local")))
    //     );
    //     install(
    //         new FactoryModuleBuilder()
    //             .implement(Configurator.class, RemoteConfigurator.class)
    //             .build(Key.get(ConfiguratorFactory.class, Names.named("remote")))
    //     );
    //     bind(ConfiguratorFactory.class).toProvider(ConfiguratorFactoryProvider.class);
    //
    //     Key<TwillRunnerService> twillRunnerServiceKey =
    //         Key.get(TwillRunnerService.class, Constants.AppFabric.RemoteExecution.class);
    //
    //     // Bind the TwillRunner for remote execution used in isolated cluster.
    //     // The binding is added in here instead of in TwillModule is because this module can be used
    //     // in standalone env as well and it doesn't require YARN.
    //     bind(twillRunnerServiceKey).to(RemoteExecutionTwillRunnerService.class).in(Scopes.SINGLETON);
    //
    //     // Bind ProgramRunnerFactory and expose it with the RemoteExecution annotation
    //     Key<ProgramRunnerFactory> programRunnerFactoryKey = Key.get(ProgramRunnerFactory.class,
    //         Constants.AppFabric.RemoteExecution.class);
    //     // ProgramRunnerFactory should be in distributed mode
    //     bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.DISTRIBUTED);
    //     bind(programRunnerFactoryKey).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
    //
    //     // The following are bindings are for ProgramRunners. They are private to this module and only
    //     // available to the remote execution ProgramRunnerFactory exposed.
    //
    //     // This set of program runners are for isolated mode
    //     bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
    //     // No need to publish program state for remote execution runs since they will publish states and get
    //     // collected back via the runtime monitoring
    //     bindConstant().annotatedWith(Names.named(DefaultProgramRunnerFactory.PUBLISH_PROGRAM_STATE)).to(false);
    //     // TwillRunner used by the ProgramRunner is the remote execution one
    //     bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class).to(twillRunnerServiceKey);
    //     // ProgramRunnerFactory used by ProgramRunner is the remote execution one.
    //     bind(ProgramRunnerFactory.class)
    //         .annotatedWith(Constants.AppFabric.ProgramRunner.class)
    //         .to(programRunnerFactoryKey);
    //
    //     // A private Map binding of ProgramRunner for ProgramRunnerFactory to use
    //     MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
    //         binder(), ProgramType.class, ProgramRunner.class);
    //
    //     defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(
    //         DistributedMapReduceProgramRunner.class);
    //     defaultProgramRunnerBinder.addBinding(ProgramType.WORKFLOW).to(
    //         DistributedWorkflowProgramRunner.class);
    //     defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
    //     Multibinder<ProgramCompletionNotifier> multiBinder = Multibinder.newSetBinder(binder(),
    //         ProgramCompletionNotifier.class);
    //     multiBinder.addBinding().toProvider(ProgramCompletionNotifierProvider.class);
    //   }
    // });
    //
    // // If MasterEnvironment is not available, assuming it is the old hadoop stack with ZK, Kafka
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
    //
    // if (masterEnv == null) {
    //   modules.add(new ZKClientModule());
    //   modules.add(new ZKDiscoveryModule());
    //   modules.add(new KafkaClientModule());
    //   modules.add(new KafkaLogAppenderModule());
    // } else {
    //   modules.add(new AbstractModule() {
    //     @Override
    //     protected void configure() {
    //       bind(DiscoveryService.class)
    //           .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
    //       bind(DiscoveryServiceClient.class)
    //           .toProvider(
    //               new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
    //     }
    //   });
    //   modules.add(new RemoteLogAppenderModule());
    //
    //   // if (coreSecurityModule.requiresZKClient()) {
    //   //   modules.add(new ZKClientModule());
    //   // }
    // }

    List<Module> modules = new ArrayList<>();
    modules.add(new RemoteTwillModule());
    modules.add(new ConfigModule(cConf, hConf, sConf));
    modules.add(RemoteAuthenticatorModules.getDefaultModule());
    modules.add(new PreviewConfigModule(cConf, hConf, sConf));
    modules.add(new IOModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(DiscoveryService.class)
            .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
        bind(DiscoveryServiceClient.class)
            .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
      }
    });
    modules.add(new RemoteLogAppenderModule());

    CoreSecurityModule coreSecurityModule = new FileBasedCoreSecurityModule() {
      @Override
      protected void bindKeyManager(Binder binder) {
        binder.bind(KeyManager.class).to(FileBasedKeyManager.class).in(Scopes.SINGLETON);
        expose(KeyManager.class);
      }
    };
    modules.add(coreSecurityModule);
    if (coreSecurityModule.requiresZKClient()) {
      modules.add(new ZKClientModule());
    }
    modules.add(new AuthenticationContextModules().getMasterModule());
    modules.addAll(Arrays.asList(
        // Always use local table implementations, which use LevelDB.
        // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
        new DataSetServiceModules().getStandaloneModules(),
        // The Dataset set modules are only needed to satisfy dependency injection
        new DataSetsModules().getStandaloneModules(),
        new MetricsStoreModule(),
        new MessagingClientModule(),
        new ExploreClientModule(),
        new AuditModule(),
        new AuthorizationModule(),
        new AuthorizationEnforcementModule().getMasterModule(),
        Modules.override(new AppFabricServiceRuntimeModule(cConf).getDistributedModules())
            .with(new AbstractModule() {
              @Override
              protected void configure() {
                bind(StorageProviderNamespaceAdmin.class).to(
                    LocalStorageProviderNamespaceAdmin.class);
              }
            }),
        new ProgramRunnerRuntimeModule().getDistributedModules(true),
        new MonitorHandlerModule(false),
        new SecureStoreServerModule(),
        new OperationalStatsModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            install(new StorageModule());
            install(new TransactionExecutorModule());
            bind(TransactionSystemClientService.class).to(DelegatingTransactionSystemClientService.class);
            bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
          }
        },
        new DFSLocationModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
            bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);

            bind(TetheringClientSubscriberService.class).in(Scopes.SINGLETON);
          }
        }
    ));

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
    LOG.debug("Injector in TaskWorkerTwillRunnable: {}", injector);
    taskWorker = injector.getInstance(TaskWorkerService.class);
  }
}
