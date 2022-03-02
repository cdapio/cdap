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
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.deploy.Dispatcher;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.guice.ImpersonatedTwillRunnerService;
import io.cdap.cdap.app.guice.NamespaceAdminModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.RemoteExecutionProgramRunnerModule;
import io.cdap.cdap.app.guice.RemoteExecutionProgramRunnerModule.ProgramCompletionNotifierProvider;
import io.cdap.cdap.app.guice.RemoteTwillModule;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.app.guice.UnsupportedExploreClient;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.DefaultNamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules.DatasetMdsProvider;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.MetadataStorageProvider;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import io.cdap.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import io.cdap.cdap.data2.metadata.AuditMetadataStorage;
import io.cdap.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.LineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.field.DefaultFieldLineageReader;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageReader;
import io.cdap.cdap.data2.metadata.writer.BasicLineageWriter;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriterDatasetFramework;
import io.cdap.cdap.data2.registry.BasicUsageRegistry;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.data2.registry.UsageWriter;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactoryProvider;
import io.cdap.cdap.internal.app.deploy.DispatcherFactory;
import io.cdap.cdap.internal.app.deploy.DispatcherFactoryProvider;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.InMemoryDispatcher;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.internal.app.deploy.RemoteConfigurator;
import io.cdap.cdap.internal.app.deploy.RemoteDispatcher;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.DistributedStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
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
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.runtime.schedule.DistributedTimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import io.cdap.cdap.internal.app.runtime.schedule.store.TriggerMisfireLogger;
import io.cdap.cdap.internal.app.services.ProgramCompletionNotifier;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.capability.CapabilityModule;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metadata.RemotePreferencesFetcherInternal;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.security.TokenSecureStoreRenewer;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.FileBasedKeyManager;
import io.cdap.cdap.security.auth.KeyManager;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.security.impersonation.RemoteUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.DefaultOwnerStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.Configs;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactories;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.quartz.SchedulerException;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
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

  public static final String BASE_DATASET_FRAMEWORK = "basicDatasetFramework";
  public static final String SPI_BASE_IMPL = "spiBaseImplementation";

  public TaskWorkerTwillRunnable(String cConfFileName, String hConfFileName, String sConfFileName) {
    super(ImmutableMap.of("cConf", cConfFileName, "hConf", hConfFileName, "sConf", sConfFileName));
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf, Configuration hConf, SConfiguration sConf) {
    List<Module> modules = new ArrayList<>();

    // CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    // modules.add(new AppFabricServiceRuntimeModule(cConf));
    // modules.add(new TwillModule());
    modules.add(new ConfigModule(cConf, hConf, sConf));
    modules.add(RemoteAuthenticatorModules.getDefaultModule());
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
    modules.add(new NamespaceAdminModule().getDistributedModules());
    // modules.add(new DataSetServiceModules().getDistributedModules());
    // modules.add(new DataSetsModules().getDistributedModules());
    modules.add(new MetadataReaderWriterModules().getDistributedModules());
    modules.add(new CapabilityModule());
    // TwillModule
    modules.add(new RemoteTwillModule());
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

        install(
            new FactoryModuleBuilder()
                .implement(new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                           },
                    new TypeLiteral<LocalApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                    })
                .build(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
                })
        );
        bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        bind(CoreSchedulerService.class).in(Scopes.SINGLETON);
        bind(Scheduler.class).to(CoreSchedulerService.class);
        bind(TimeSchedulerService.class).to(DistributedTimeSchedulerService.class).in(Scopes.SINGLETON);
        bind(PreferencesFetcher.class).to(RemotePreferencesFetcherInternal.class).in(Scopes.SINGLETON);
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS"))
            .toProvider(DatasetMdsProvider.class).in(Singleton.class);
        bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);
        MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(
            binder(), String.class, DatasetModule.class, Constants.Dataset.Manager.DefaultDatasetModules.class);
        // NOTE: order is important due to dependencies between modules
        mapBinder.addBinding("orderedTable-memory").toInstance(new InMemoryTableModule());
        mapBinder.addBinding("metricsTable-memory").toInstance(new InMemoryMetricsTableModule());
        bind(DatasetDefinitionRegistryFactory.class)
            .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

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
        bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class).in(Scopes.SINGLETON);

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
        bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
        bind(ExploreClient.class).to(UnsupportedExploreClient.class);

        // DataSetsModules bindings
        bind(MetadataStorage.class).annotatedWith(Names.named(SPI_BASE_IMPL))
            .toProvider(MetadataStorageProvider.class).in(Scopes.SINGLETON);
        bind(MetadataStorage.class).to(AuditMetadataStorage.class).in(Scopes.SINGLETON);
        bind(DatasetFramework.class)
            .annotatedWith(Names.named(BASE_DATASET_FRAMEWORK))
            .to(RemoteDatasetFramework.class);
        bind(LineageStoreReader.class).to(DefaultLineageStoreReader.class);
        bind(FieldLineageReader.class).to(DefaultFieldLineageReader.class);
        bind(LineageWriter.class).to(BasicLineageWriter.class);
        bind(FieldLineageWriter.class).to(BasicLineageWriter.class);
        bind(UsageRegistry.class).to(BasicUsageRegistry.class).in(Scopes.SINGLETON);
        bind(UsageWriter.class).to(BasicUsageRegistry.class).in(Scopes.SINGLETON);
        bind(BasicUsageRegistry.class).in(Scopes.SINGLETON);
        bind(DatasetFramework.class).to(LineageWriterDatasetFramework.class);
        bind(DefaultOwnerStore.class).in(Scopes.SINGLETON);
        bind(OwnerStore.class).to(DefaultOwnerStore.class);
      }

      /**
       * Provides a supplier of quartz scheduler so that initialization of the scheduler can be done after guice
       * injection. It returns a singleton of Scheduler.
       */
      @Provides
      @SuppressWarnings("unused")
      public Supplier<org.quartz.Scheduler> providesSchedulerSupplier(final DatasetBasedTimeScheduleStore scheduleStore,
          final CConfiguration cConf) {
        return new Supplier<org.quartz.Scheduler>() {
          private org.quartz.Scheduler scheduler;

          @Override
          public synchronized org.quartz.Scheduler get() {
            try {
              if (scheduler == null) {
                scheduler = getScheduler(scheduleStore, cConf);
              }
              return scheduler;
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        };
      }

      /**
       * Create a quartz scheduler. Quartz factory method is not used, because inflexible in allowing custom
       * jobstore and turning off check for new versions.
       *
       * @param store JobStore.
       * @param cConf CConfiguration.
       * @return an instance of {@link org.quartz.Scheduler}
       */
      private org.quartz.Scheduler getScheduler(JobStore store, CConfiguration cConf) throws SchedulerException {
        int threadPoolSize = cConf.getInt(Constants.Scheduler.CFG_SCHEDULER_MAX_THREAD_POOL_SIZE);
        ExecutorThreadPool threadPool = new ExecutorThreadPool(threadPoolSize);
        threadPool.initialize();
        String schedulerName = DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME;
        String schedulerInstanceId = DirectSchedulerFactory.DEFAULT_INSTANCE_ID;

        QuartzSchedulerResources qrs = new QuartzSchedulerResources();
        JobRunShellFactory jrsf = new StdJobRunShellFactory();

        qrs.setName(schedulerName);
        qrs.setInstanceId(schedulerInstanceId);
        qrs.setJobRunShellFactory(jrsf);
        qrs.setThreadPool(threadPool);
        qrs.setThreadExecutor(new DefaultThreadExecutor());
        qrs.setJobStore(store);
        qrs.setRunUpdateCheck(false);
        QuartzScheduler qs = new QuartzScheduler(qrs, -1, -1);

        ClassLoadHelper cch = new CascadingClassLoadHelper();
        cch.initialize();

        store.initialize(cch, qs.getSchedulerSignaler());
        org.quartz.Scheduler scheduler = new StdScheduler(qs);

        jrsf.initialize(scheduler);
        qs.initialize();

        scheduler.getListenerManager().addTriggerListener(new TriggerMisfireLogger());
        return scheduler;
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
