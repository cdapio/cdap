package io.cdap.cdap.master.export_import;

import static io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO;

import com.google.api.client.util.Throwables;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.artifact.ApplicationClass;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule.ServiceType;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule.UgiProviderProvider;
import io.cdap.cdap.app.guice.AuditLogWriterModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.NamespaceAdminModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.id.Id.Artifact;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactoryProvider;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.NamespaceResourceDeleter;
import io.cdap.cdap.internal.app.namespace.NoopNamespaceResourceDeleter;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.AuthorizationArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.LocalArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import io.cdap.cdap.internal.app.runtime.schedule.DistributedTimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.RemoteScheduleManager;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleManager;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import io.cdap.cdap.internal.app.runtime.schedule.store.TriggerMisfireLogger;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.capability.CapabilityModule;
import io.cdap.cdap.internal.operation.guice.OperationModule;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.internal.profile.AdminEventPublisher;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.metadata.LocalPreferencesFetcherInternal;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.operations.guice.OperationalStatsModule;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.store.secretmanager.SecretManagerSecureStoreService;
import io.cdap.cdap.sourcecontrol.guice.SourceControlModule;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.NamespaceTable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.google.common.base.Supplier;
import java.util.stream.Collectors;
import org.apache.curator.shaded.com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
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

public class ImporterMain {

  private static final Logger LOG = LoggerFactory.getLogger(ImporterMain.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  public static void main(String[] args) {
    LOG.debug("Received arguments: {}", Arrays.toString(args));

    if (args.length < 1) {
      LOG.error("Usage: ImporterMain <gcs-bucket-uri>");
      LOG.error("Example: ImporterMain gs://my-backup-bucket/run-123");
      System.exit(1);
    }

    String gcsBackupPath = "gs://"+ args[0] +"/";
    JobReport report = new JobReport(JobReport.JobType.IMPORT);
    SecretManagerSecureStoreService secretManagerSecureStoreService = null;
    Scheduler schedulerService = null;

    try {
      Injector injector = initializeInjector();
      secretManagerSecureStoreService = injector.getInstance(SecretManagerSecureStoreService.class);
      secretManagerSecureStoreService.startAndWait();
      schedulerService = injector.getInstance(Scheduler.class);
      if (schedulerService instanceof Service) {
        ((Service) schedulerService).startAndWait();
      }

      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
      AdminEventPublisher adminEventPublisher = new AdminEventPublisher(injector.getInstance(CConfiguration.class),
          new MultiThreadMessagingContext(injector.getInstance(MessagingService.class)));
      ArtifactStore artifactStore = injector.getInstance(ArtifactStore.class);
      Impersonator impersonator = injector.getInstance(Impersonator.class);
      SecureStoreManager secureStoreManager = injector.getInstance(SecureStoreManager.class);

      // Open report streams at the beginning of the job
      report.open(locationFactory, gcsBackupPath);
      Location baseLocation = locationFactory.create(gcsBackupPath);

      // Import Namespaces and update the report
      List<NamespaceMeta> importedNamespaces = importNamespaces(transactionRunner, locationFactory, gcsBackupPath, report);
      importPipelines(importedNamespaces, transactionRunner, locationFactory, gcsBackupPath, report, adminEventPublisher);
      importUserPlugins(importedNamespaces, artifactStore, locationFactory, gcsBackupPath, report, impersonator);
      importSystemPlugins(artifactStore, baseLocation, report, impersonator);
      importSecureKeys(importedNamespaces, secureStoreManager, baseLocation, report);
      importSchedules(importedNamespaces, schedulerService, baseLocation, report);

      LOG.info("All import tasks finished.");

    } catch (Exception e) {
      LOG.error("Job failed with an unrecoverable error during setup or execution.", e);
      System.exit(1);
    }  finally {
      if (schedulerService instanceof Service) {
        ((Service) schedulerService).stopAndWait();
      }
      if (secretManagerSecureStoreService != null) {
        secretManagerSecureStoreService.stopAndWait();
      }
      report.close();
      LOG.info("Job finished.");
    }
  }

  private static Injector initializeInjector() throws MalformedURLException {
    LOG.info("Initializing Guice injector...");

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = new Configuration();
    File hConfFile = new File("/etc/hadoop/conf/core-site.xml");
    if (hConfFile.exists()) {
      hConf.addResource(hConfFile.toURI().toURL());
      LOG.info("Loaded hConf from {}", hConfFile.getAbsolutePath());
    }

    // --- FIX: Programmatically set the GCS connector properties ---
    // This ensures Hadoop knows how to handle the "gs://" scheme, even if
    // the mounted core-site.xml is missing these properties.
    // hConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    hConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    // The following property is needed to enable service account authentication via Workload Identity.
    hConf.setBoolean("fs.gs.auth.service.account.enable", true);
    CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    List<Module> modules = new ArrayList<>(Arrays.asList(
        new ConfigModule(cConf, hConf),
        new StorageModule(),
        new TransactionExecutorModule(),
        coreSecurityModule,
        new IOModule(),
        new DFSLocationModule(),
        new SecureStoreServerModule(),
        new CapabilityModule(),
        new ProvisionerModule(),
        RemoteAuthenticatorModules.getDefaultModule(),
        new AuthenticationContextModules().getMasterModule(),
        new NamespaceAdminModule().getDistributedModules(),
        new MessagingServiceModule(cConf),
        new DataSetsModules().getStandaloneModules(),
        new InMemoryDiscoveryModule(),
        new OperationModule(),
        new SourceControlModule(),
        new TwillModule(),
        new UserCredentialAeadEncryptionModule(),
        new AuthorizationEnforcementModule().getDistributedModules(),
        new AuditLogWriterModule(cConf).getDistributedModules(),
        new DataSetServiceModules().getDistributedModules(),
        new ProgramRunnerRuntimeModule().getDistributedModules(true),
        new MetricsClientRuntimeModule().getDistributedModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            // bind(AccessEnforcer.class).to(NoOpAccessController.class);
            // bind(NamespaceResourceDeleter.class).to(NoopNamespaceResourceDeleter.class)
            //     .in(Scopes.SINGLETON);
            bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
            bind(Store.class).to(DefaultStore.class);
            bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
            // bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class);
            bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
            // bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.clasecurestore.css)
            //     .in(Scopes.SINGLETON);
            bind(ArtifactStore.class).in(Scopes.SINGLETON);
            bind(SecretStore.class).to(DefaultSecretStore.class).in(Scopes.SINGLETON);
            bind(CoreSchedulerService.class).in(Scopes.SINGLETON);
            bind(Scheduler.class).to(CoreSchedulerService.class);
            // bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
            bind(ProgramStatePublisher.class).to(MessagingProgramStatePublisher.class)
                .in(Scopes.SINGLETON);
            bind(TimeSchedulerService.class).to(DistributedTimeSchedulerService.class)
                .in(Scopes.SINGLETON);
            bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
            bind(PreferencesFetcher.class).to(LocalPreferencesFetcherInternal.class)
                .in(Scopes.SINGLETON);
            bind(ScheduleManager.class).to(RemoteScheduleManager.class).in(Scopes.SINGLETON);
            // bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class)
            //     .in(Scopes.SINGLETON);
            bind(JobStore.class).to(DatasetBasedTimeScheduleStore.class);
            bind(PluginFinder.class).to(LocalPluginFinder.class);
            bind(TransactionSystemClientService.class).to(
                DelegatingTransactionSystemClientService.class);
            bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
            bind(ConfiguratorFactory.class).toProvider(ConfiguratorFactoryProvider.class);
            bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);
            // Add the private module for ArtifactRepository
            install(new PrivateModule() {
              @Override
              protected void configure() {
                bind(ArtifactRepositoryReader.class).to(LocalArtifactRepositoryReader.class)
                    .in(Scopes.SINGLETON);
                expose(ArtifactRepositoryReader.class);

                bind(ArtifactRepository.class)
                    .annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO))
                    .to(DefaultArtifactRepository.class)
                    .in(Scopes.SINGLETON);
                expose(ArtifactRepository.class).annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO));

                bind(ArtifactRepository.class).to(AuthorizationArtifactRepository.class)
                    .in(Scopes.SINGLETON);
                expose(ArtifactRepository.class);
              }
            });
            install(
                new FactoryModuleBuilder()
                    .implement(
                        new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                        },
                        new TypeLiteral<LocalApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                        })
                    .build(
                        new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
                        })
            );
          }

          // @Provides
          // @SuppressWarnings("unused")
          // public Supplier<org.quartz.Scheduler> providesSchedulerSupplier(
          //     final JobStore scheduleStore,
          //     final CConfiguration cConf) {
          //   return () -> {
          //     try {
          //       return getScheduler(scheduleStore, cConf);
          //     } catch (SchedulerException | org.quartz.SchedulerException e) {
          //       throw Throwables.propagate(e);
          //     }
          //   };
          // }
          @Provides
          @SuppressWarnings("unused")
          // Corrected return type to com.google.common.base.Supplier
          public Supplier<org.quartz.Scheduler> providesSchedulerSupplier(
              final Injector injector,
              final CConfiguration cConf) {
            // Explicitly create an instance of com.google.common.base.Supplier
            return new Supplier<org.quartz.Scheduler>() {
              @Override
              public org.quartz.Scheduler get() {
                try {
                  JobStore scheduleStore = injector.getInstance(JobStore.class);
                  return getScheduler(scheduleStore, cConf);
                } catch (SchedulerException | org.quartz.SchedulerException e) {
                  // Use com.google.common.base.Throwables.propagate
                  throw Throwables.propagate(e);
                }
              }
            };
          }
        }
    ));
    Injector injector = Guice.createInjector(modules);

    LOG.info("Guice injector created successfully.");
    return injector;
  }

  private static org.quartz.Scheduler getScheduler(JobStore store,
      CConfiguration cConf) throws SchedulerException, org.quartz.SchedulerException {
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

  // private static Injector initializeInjector() throws MalformedURLException {
  //   LOG.info("Initializing Guice injector...");
  //   CConfiguration cConf = CConfiguration.create();
  //   Configuration hConf = new Configuration();
  //   File hConfFile = new File("/etc/hadoop/conf/core-site.xml");
  //   if (hConfFile.exists()) {
  //     hConf.addResource(hConfFile.toURI().toURL());
  //     LOG.info("Loaded hConf from {}", hConfFile.getAbsolutePath());
  //   }
  //
  //   hConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
  //   hConf.setBoolean("fs.gs.auth.service.account.enable", true);
  //
  //   List<Module> modules = new ArrayList<>(Arrays.asList(
  //       new ConfigModule(cConf, hConf),
  //       new StorageModule(),
  //       new DFSLocationModule(),
  //       new CapabilityModule(),
  //       new IOModule(),
  //       // new ProvisionerModule(),
  //       new AuditLogWriterModule(cConf).getDistributedModules(),
  //       RemoteAuthenticatorModules.getDefaultModule(),
  //       new MetricsClientRuntimeModule().getDistributedModules(),
  //       new UserCredentialAeadEncryptionModule(),
  //       CoreSecurityRuntimeModule.getDistributedModule(cConf),
  //       // new SecureStoreServerModule(),
  //       // new AuthenticationContextModules().getMasterModule(),
  //       // // new NamespaceAdminModule().getDistributedModules(),
  //       // new MessagingServiceModule(cConf),
  //       // new DataSetsModules().getStandaloneModules(),
  //       // new InMemoryDiscoveryModule(),
  //       // new AbstractModule() {
  //       //   @Override
  //       //   protected void configure() {
  //       //     bind(AccessEnforcer.class).to(NoOpAccessController.class);
  //       //     bind(NamespaceResourceDeleter.class).to(NoopNamespaceResourceDeleter.class)
  //       //         .in(Scopes.SINGLETON);
  //       //     bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
  //       //     bind(Store.class).to(DefaultStore.class);
  //       //     bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
  //       //     bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class);
  //       //     bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
  //       //     bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
  //       //         .in(Scopes.SINGLETON);
  //       //     bind(ArtifactStore.class).in(Scopes.SINGLETON);
  //       //     bind(SecretStore.class).to(DefaultSecretStore.class).in(Scopes.SINGLETON);
  //       //     bind(CoreSchedulerService.class).in(Scopes.SINGLETON);
  //       //     bind(Scheduler.class).to(CoreSchedulerService.class);
  //       //     bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
  //       //     bind(TimeSchedulerService.class).to(DistributedTimeSchedulerService.class)
  //       //         .in(Scopes.SINGLETON);
  //       //     bind(PreferencesFetcher.class).to(LocalPreferencesFetcherInternal.class).in(Scopes.SINGLETON);
  //       //     // Add the private module for ArtifactRepository
  //       //     install(new PrivateModule() {
  //       //       @Override
  //       //       protected void configure() {
  //       //         bind(ArtifactRepositoryReader.class).to(LocalArtifactRepositoryReader.class)
  //       //             .in(Scopes.SINGLETON);
  //       //         expose(ArtifactRepositoryReader.class);
  //       //
  //       //         bind(ArtifactRepository.class)
  //       //             .annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO))
  //       //             .to(DefaultArtifactRepository.class)
  //       //             .in(Scopes.SINGLETON);
  //       //         expose(ArtifactRepository.class).annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO));
  //       //
  //       //         bind(ArtifactRepository.class).to(AuthorizationArtifactRepository.class)
  //       //             .in(Scopes.SINGLETON);
  //       //         expose(ArtifactRepository.class);
  //       //       }
  //       //     });
  //       //   }
  //       // }
  //
  //       // AppFabricServiceMain
  //       new DataSetServiceModules().getStandaloneModules(),
  //       // The Dataset set modules are only needed to satisfy dependency injection
  //       new DataSetsModules().getStandaloneModules(),
  //       new MetricsStoreModule(),
  //       new MessagingServiceModule(cConf),
  //       new AuditModule(),
  //       new AuthorizationModule(),
  //       new AuthorizationEnforcementModule().getMasterModule(),
  //       Modules.override(new AppFabricServiceRuntimeModule(cConf, ServiceType.SERVER).getDistributedModules())
  //           .with(new AbstractModule() {
  //             @Override
  //             protected void configure() {
  //               bind(StorageProviderNamespaceAdmin.class).to(
  //                   LocalStorageProviderNamespaceAdmin.class);
  //             }
  //           }),
  //       new ProgramRunnerRuntimeModule().getDistributedModules(true),
  //       new MonitorHandlerModule(false, cConf),
  //       new SecureStoreServerModule(),
  //       new OperationalStatsModule(),
  //       getDataFabricModule(),
  //       new DFSLocationModule(),
  //       new AbstractModule() {
  //         @Override
  //         protected void configure() {
  //           bind(TwillRunner.class).to(TwillRunnerService.class);
  //           bind(MetadataPublisher.class).to(MessagingMetadataPublisher.class);
  //           bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
  //         }
  //       }
  //   ));
  //   return Guice.createInjector(modules);
  // }

  /**
   * Scans a GCS path for namespace backups and imports them.
   */
  public static List<NamespaceMeta> importNamespaces(TransactionRunner transactionRunner, LocationFactory locationFactory,
      String backupPath, JobReport report) {
    LOG.info("Starting import of all namespaces...");
    List<NamespaceMeta> importedNamespaces = new ArrayList<>();
    Location namespacesDir;
    try {
      namespacesDir = locationFactory.create(backupPath).append("namespaces");
      if (!namespacesDir.exists()) {
        LOG.warn("Namespaces directory does not exist, skipping namespace import: {}", namespacesDir.toURI());
        return Collections.emptyList();
      }
    } catch (IOException e) {
      LOG.error("FATAL: Could not access namespaces directory in GCS.", e);
      return Collections.emptyList();
    }

    try {
      Collection<Location> namespaceDirs = namespacesDir.list();
      LOG.info("Found {} potential namespace backups to import.", namespaceDirs.size());

      for (Location namespaceDir : namespaceDirs) {
        if (!namespaceDir.isDirectory()) {
          LOG.debug("Skipping non-directory item: {}", namespaceDir.toURI());
          continue;
        }
        String namespaceId = namespaceDir.getName();
        try {
          Location namespaceFile = namespaceDir.append("namespaceMeta.json");
          if (!namespaceFile.exists()) {
            LOG.warn("Skipping directory '{}' because it is missing namespaceMeta.json", namespaceId);
            report.addFailure("Namespace", namespaceId, "N/A", "namespaceMeta.json not found in backup directory");
            continue;
          }

          // Read and parse the namespace meta from GCS
          NamespaceMeta namespaceMeta;
          try (InputStream in = namespaceFile.getInputStream();
              Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            namespaceMeta = GSON.fromJson(reader, NamespaceMeta.class);
          }

          // Write the namespace to the CDAP store
          TransactionRunners.run(transactionRunner, context -> {
            NamespaceTable namespaceTable = new NamespaceTable(context);
            namespaceTable.create(namespaceMeta);
          });

          importedNamespaces.add(namespaceMeta);
          LOG.info("Successfully imported namespace '{}'", namespaceId);
          report.addSuccess("Namespace", namespaceId, "N/A", namespaceFile.toURI().toString());

        } catch (Exception e) {
          LOG.error("Failed to import namespace '{}'", namespaceId, e);
          report.addFailure("Namespace", namespaceId, "N/A", e.getMessage());
        }
      }
    } catch (IOException e) {
      LOG.error("FATAL: Failed to list contents of namespaces directory: {}", namespacesDir.toURI(), e);
    }
    return importedNamespaces;
  }
  protected static final Module getDataFabricModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(new StorageModule());
        install(new TransactionExecutorModule());

        // Bind transaction system to a constant one, basically no transaction, with every write become
        // visible immediately.
        // TODO: Ideally we shouldn't need this at all. However, it is needed now to satisfy dependencies
        bind(TransactionSystemClientService.class).to(
            DelegatingTransactionSystemClientService.class);
        bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
      }
    };
  }

  public static void importPipelines(List<NamespaceMeta> namespaces, TransactionRunner transactionRunner,
      LocationFactory locationFactory, String backupPath, JobReport report, AdminEventPublisher adminEventPublisher) throws IOException {
    LOG.info("Starting import of all pipelines...");

    for (NamespaceMeta namespace : namespaces) {
      Location pipelinesBackupDir = locationFactory.create(backupPath).append("namespaces")
          .append(namespace.getName()).append("pipelines");

      if (!pipelinesBackupDir.exists()) {
        LOG.debug("No pipelines directory found for namespace '{}', skipping.", namespace.getName());
        continue;
      }

      // Iterate through each pipeline directory (e.g., /pipelines/myETL)
      for (Location pipelineDir : pipelinesBackupDir.list()) {
        if (!pipelineDir.isDirectory()) continue;

        String pipelineName = pipelineDir.getName();
        try {
          // Iterate through each version directory inside the pipeline directory
          for (Location versionDir : pipelineDir.list()) {
            if (!versionDir.isDirectory()) continue;

            String versionName = versionDir.getName();
            String sourcePath = "";

            try {
              Location pipelineFile = versionDir.append("pipeline.json");
              sourcePath = pipelineFile.toURI().toString();
              if (!pipelineFile.exists()) {
                report.addFailure("Pipeline", String.format("%s (v%s)", pipelineName, versionName), namespace.getName(), "pipeline.json not found");
                continue;
              }

              ApplicationMeta appMeta;
              try (Reader reader = new InputStreamReader(pipelineFile.getInputStream(), StandardCharsets.UTF_8)) {
                appMeta = GSON.fromJson(reader, ApplicationMeta.class);
              }

              ApplicationId appId = new ApplicationId(namespace.getName(), pipelineName, versionName);
              boolean isLatest = appMeta.getChange() != null && appMeta.getChange().getLatest();

              ApplicationMeta finalAppMeta = appMeta;
              TransactionRunners.run(transactionRunner, context -> {
                AppMetadataStore appStore = AppMetadataStore.create(context);
                // Note: The data store method returns an int, but we don't need to use it.
                appStore.createApplicationVersion(appId, finalAppMeta, isLatest);
              }, IOException.class, Exception.class);

              // adminEventPublisher.publishAppCreation(appId, appMeta.getSpec());
              report.addSuccess("Pipeline", String.format("%s (v%s)", pipelineName, versionName), namespace.getName(), sourcePath);
              LOG.info("Successfully imported pipeline '{}' version '{}' into namespace '{}'", pipelineName, versionName, namespace.getName());

            } catch (Exception e) {
              LOG.error("Failed to import pipeline '{}' version '{}' in namespace '{}'", pipelineName, versionName, namespace.getName(), e);
              report.addFailure("Pipeline", String.format("%s (v%s)", pipelineName, versionName), namespace.getName(), e.getMessage());
            }
          }
        } catch (Exception e) {
          LOG.error("Failed to process pipeline directory '{}' in namespace '{}'", pipelineName, namespace.getName(), e);
          report.addFailure("Pipeline-Batch", pipelineName, namespace.getName(), "Failed to list versions: " + e.getMessage());
        }
      }
    }
  }

  public static void importUserPlugins(List<NamespaceMeta> namespaces, ArtifactStore artifactStore,
      LocationFactory locationFactory, String backupPath, JobReport report, Impersonator impersonator) {
    LOG.info("Starting import of user plugins...");

    for (NamespaceMeta namespace : namespaces) {
      String namespaceName = namespace.getName();
      NamespaceId namespaceId = new NamespaceId(namespaceName);
      LOG.info("Starting user plugin import for namespace '{}'.", namespaceName);

      try {
        Location pluginsBackupDir = locationFactory.create(backupPath).append("namespaces").append(namespaceName).append("plugins");
        if (!pluginsBackupDir.exists()) {
          LOG.debug("No plugins directory found for namespace '{}', skipping.", namespaceName);
          continue;
        }

        for (Location artifactDir : pluginsBackupDir.list()) {
          if (!artifactDir.isDirectory()) continue;
          String artifactName = artifactDir.getName();

          for (Location versionDir : artifactDir.list()) {
            if (!versionDir.isDirectory()) continue;
            String artifactVersion = versionDir.getName();
            String artifactIdStr = String.format("%s:%s", artifactName, artifactVersion);

            File tempJarFile = null;
            try {
              Location metaFile = versionDir.append("meta.json");
              if (!metaFile.exists()) {
                report.addFailure("Plugin", artifactIdStr, namespaceName, "meta.json not found.");
                continue;
              }

              ArtifactDetail detail;
              try (Reader reader = new InputStreamReader(metaFile.getInputStream(), StandardCharsets.UTF_8)) {
                detail = GSON.fromJson(reader, ArtifactDetail.class);
              }

              LOG.debug("detail:{}", detail.toString());

              // Correctly find the JAR file within the backup directory, not from the meta's location URI
              Location sourceJarLocation = null;
              for (Location fileInVersionDir : versionDir.list()) {
                if (fileInVersionDir.getName().endsWith(".jar")) {
                  sourceJarLocation = fileInVersionDir;
                  break;
                }
              }

              if (sourceJarLocation == null) {
                report.addFailure("Plugin", artifactIdStr, namespaceName, "JAR file not found in backup directory.");
                continue;
              }
              tempJarFile = File.createTempFile(artifactName, ".jar");
              Files.asByteSink(tempJarFile).writeFrom(sourceJarLocation.getInputStream());

              // Using the low-level ArtifactStore.write() method as requested
              io.cdap.cdap.common.id.Id.Artifact artifactId =
                  Id.Artifact.fromEntityId(namespaceId.artifact(artifactName, artifactVersion));

              artifactStore.write(artifactId, detail.getMeta(), tempJarFile, new EntityImpersonator(artifactId.toEntityId(), impersonator));

              // Second, write the artifact properties as a separate step.
              if (detail.getMeta().getProperties() != null && !detail.getMeta().getProperties().isEmpty()) {
                artifactStore.updateArtifactProperties(artifactId, oldProps -> detail.getMeta().getProperties());
              }

              report.addSuccess("Plugin", artifactIdStr, namespaceName, sourceJarLocation.toURI().toString());
              LOG.info("Successfully imported plugin '{}' in namespace '{}'", artifactIdStr, namespaceName);

            } catch (Exception e) {
              LOG.error("Failed to import plugin '{}' in namespace '{}'", artifactIdStr, namespaceName, e);
              report.addFailure("Plugin", artifactIdStr, namespaceName, e.getMessage());
            } finally {
              if (tempJarFile != null && tempJarFile.exists()) {
                tempJarFile.delete();
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to process plugins directory in namespace '{}'", namespaceName, e);
        report.addFailure("Plugin-Batch", "ALL", namespaceName, "Failed to list plugins: " + e.getMessage());
      }
    }
    LOG.info("Finished importing user plugins.");
  }

  /**
   * Imports all secure keys, which will be re-encrypted with the destination KMS key.
   */
  public static void importSecureKeys(List<NamespaceMeta> namespaces, SecureStoreManager secureStoreManager,
      Location baseLocation, JobReport report) {
    LOG.info("Starting import of secure keys...");
    for (NamespaceMeta namespace : namespaces) {
      String namespaceName = namespace.getName();
      LOG.info("Starting secure key import for namespace '{}'.", namespaceName);

      try {
        Location secureKeysDir = baseLocation.append("namespaces").append(namespaceName).append("securekeys");
        if (!secureKeysDir.exists()) {
          LOG.debug("No secure keys directory found for namespace '{}', skipping.", namespaceName);
          continue;
        }

        for (Location keyFile : secureKeysDir.list()) {
          if (!keyFile.getName().endsWith(".json")) continue;

          String keyName = keyFile.getName().replace(".json", "");
          try {
            SecureKeyExportDTO secretToImport;
            try (Reader reader = new InputStreamReader(keyFile.getInputStream(), StandardCharsets.UTF_8)) {
              secretToImport = GSON.fromJson(reader, SecureKeyExportDTO.class);
            }

            // The store method will handle re-encrypting the data with the new environment's KMS key
            secureStoreManager.put(namespaceName, secretToImport.getMetadata().getName(), secretToImport.getDataB64(),
                secretToImport.getMetadata().getDescription(),
                secretToImport.getMetadata().getProperties());

            report.addSuccess("SecureKey", keyName, namespaceName, keyFile.toURI().toString());
            LOG.info("Successfully imported secure key '{}' into namespace '{}'", keyName, namespaceName);

          } catch (Exception e) {
            LOG.error("Failed to import secure key '{}' in namespace '{}'", keyName, namespaceName, e);
            report.addFailure("SecureKey", keyName, namespaceName, e.getMessage());
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to process secure keys directory in namespace '{}'", namespaceName, e);
        report.addFailure("SecureKey-Batch", "ALL", namespaceName, "Failed to list keys: " + e.getMessage());
      }
    }
    LOG.info("Finished importing secure keys.");
  }

  // SYSTEM PLUGINS

  public static void importSystemPlugins(ArtifactStore artifactStore,
      Location baseLocation, JobReport report, Impersonator impersonator) {
    LOG.info("Starting import of system plugins...");
    try {
      Location systemPluginsDir = baseLocation.append("namespaces").append("system").append("plugins");
      if (!systemPluginsDir.exists()) {
        LOG.info("No system plugins directory found, skipping.");
        return;
      }

      // Pass 1: Scan all metadata first to build dependency graph
      Map<Id.Artifact, SystemArtifactMetaInfo> systemArtifactsMeta = new HashMap<>();
      for (Location artifactDir : systemPluginsDir.list()) {
        if (!artifactDir.isDirectory()) continue;
        for (Location versionDir : artifactDir.list()) {
          if (!versionDir.isDirectory()) continue;
          SystemArtifactMetaInfo metaInfo = getSystemArtifactMetaInfo(artifactDir, versionDir);
          if (metaInfo != null) {
            systemArtifactsMeta.put(metaInfo.getArtifactId(), metaInfo);
          }
        }
      }

      // Build dependency graph from metadata
      Multimap<Artifact, Artifact> childToParents = HashMultimap.create();
      Multimap<Id.Artifact, Id.Artifact> parentToChildren = HashMultimap.create();
      Set<Id.Artifact> remainingArtifacts = new HashSet<>(systemArtifactsMeta.keySet());

      for (SystemArtifactMetaInfo childInfo : systemArtifactsMeta.values()) {
        Id.Artifact childId = childInfo.getArtifactId();
        for (SystemArtifactMetaInfo potentialParent : systemArtifactsMeta.values()) {
          Id.Artifact potentialParentId = potentialParent.getArtifactId();
          if (childInfo.hasParent(potentialParentId)) {
            childToParents.put(childId, potentialParentId);
            parentToChildren.put(potentialParentId, childId);
          }
        }
      }

      // Pass 2: Iteratively import artifacts that have no remaining dependencies
      boolean artifactsAdded = true;
      while (!remainingArtifacts.isEmpty() && artifactsAdded) {
        Set<Id.Artifact> importable = remainingArtifacts.stream()
            .filter(id -> !childToParents.containsKey(id))
            .collect(Collectors.toSet());

        if (importable.isEmpty()) {
          LOG.error("Circular dependency detected in system artifacts. Remaining: {}", Joiner.on(", ").join(remainingArtifacts));
          report.addFailure("System-Plugin-Batch", "ALL", "system", "Circular dependency detected.");
          break;
        }

        List<Future<Artifact>> futures = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(importable.size(), 50));

        for (Id.Artifact artifactId : importable) {
          futures.add(executorService.submit(() -> {
            SystemArtifactMetaInfo metaInfo = systemArtifactsMeta.get(artifactId);
            File tempJarFile = null;
            try {
              // Check if artifact already exists before downloading JAR
              try {
                artifactStore.getArtifact(metaInfo.getArtifactId());
                LOG.info("System artifact {} already exists. Skipping import.", metaInfo.getArtifactId());
                report.addSuccess("System-Plugin", metaInfo.getArtifactId().getName() + "-" + metaInfo.getArtifactId().getVersion(),
                    "system", "SKIPPED (already exists)");
                return artifactId;
              } catch (ArtifactNotFoundException e) {
                // This is expected, artifact does not exist, so we can proceed with import.
              }

              // Download JAR only when it's ready to be imported
              tempJarFile = File.createTempFile(metaInfo.getArtifactId().getName(), ".jar");
              Files.asByteSink(tempJarFile).writeFrom(metaInfo.getJarLocation().getInputStream());


              ArtifactMeta artifactMeta = new ArtifactMeta(
                  ArtifactClasses.builder().addPlugins(metaInfo.getPlugins()).addApps(metaInfo.getApps()).build(),
                  metaInfo.getParents(), metaInfo.getProperties());
              EntityImpersonator entityImpersonator = new EntityImpersonator(metaInfo.getArtifactId().toEntityId(),
                  impersonator);
              artifactStore.write(metaInfo.getArtifactId(), artifactMeta, tempJarFile, entityImpersonator);
              report.addSuccess("System-Plugin", metaInfo.getArtifactId().getName() + "-" + metaInfo.getArtifactId().getVersion(),
                  "system", metaInfo.getJarLocation().toURI().toString());
              return artifactId;
            } catch (Exception e) {
              LOG.error("Failed to import system plugin '{}'", artifactId, e);
              report.addFailure("System-Plugin", artifactId.getName() + "-" + artifactId.getVersion(), "system", e.getMessage());
              throw e;
            } finally {
              if (tempJarFile != null && tempJarFile.exists()) {
                tempJarFile.delete();
              }
            }
          }));
        }
        executorService.shutdown();

        Set<Id.Artifact> successfullyAdded = new HashSet<>();
        for (Future<Id.Artifact> future : futures) {
          try {
            successfullyAdded.add(future.get());
          } catch (Exception e) {
            LOG.error("An error occurred during parallel system artifact import.", e.getCause());
          }
        }

        remainingArtifacts.removeAll(successfullyAdded);
        for (Id.Artifact added : successfullyAdded) {
          for (Id.Artifact child : parentToChildren.get(added)) {
            childToParents.remove(child, added);
          }
        }
        artifactsAdded = !successfullyAdded.isEmpty();
      }
    } catch (IOException e) {
      LOG.error("Failed to process system plugins directory.", e);
      report.addFailure("System-Plugin-Batch", "ALL", "system", "Failed to list plugins: " + e.getMessage());
    }
    LOG.info("Finished importing system plugins.");
  }

  private static SystemArtifactMetaInfo getSystemArtifactMetaInfo(Location artifactDir, Location versionDir) throws IOException {
    String artifactName = artifactDir.getName();
    String version = versionDir.getName();
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.SYSTEM, artifactName, version);

    Location metaFile = versionDir.append("meta.json");
    if (!metaFile.exists()) {
      LOG.warn("meta.json not found for system artifact {}, skipping.", artifactId);
      return null;
    }

    ArtifactDetail detail;
    try (Reader reader = new InputStreamReader(metaFile.getInputStream(), StandardCharsets.UTF_8)) {
      detail = GSON.fromJson(reader, ArtifactDetail.class);
    }

    Location jarFileLocation = findJarFile(versionDir);
    if (jarFileLocation == null) {
      LOG.warn("JAR file not found for system artifact {}, skipping.", artifactId);
      return null;
    }

    return new SystemArtifactMetaInfo(artifactId, jarFileLocation,
        detail.getMeta().getUsableBy(),
        detail.getMeta().getClasses().getPlugins(),
        detail.getMeta().getClasses().getApps(),
        detail.getMeta().getProperties());
  }
  private static Location findJarFile(Location dir) throws IOException {
    for (Location file : dir.list()) {
      if (file.getName().endsWith(".jar")) {
        return file;
      }
    }
    return null;
  }

  // SCHEDULES
  public static void importSchedules(List<NamespaceMeta> namespaces, Scheduler scheduler,
      Location baseLocation, JobReport report) {
    LOG.info("Starting import of schedules...");
    for (NamespaceMeta namespace : namespaces) {
      String namespaceName = namespace.getName();
      LOG.info("Starting schedule import for namespace '{}'.", namespaceName);

      try {
        Location schedulesBaseDir = baseLocation.append("namespaces").append(namespaceName).append("schedules");
        if (!schedulesBaseDir.exists()) {
          LOG.debug("No schedules directory found for namespace '{}', skipping.", namespaceName);
          continue;
        }

        // Iterate through each application's schedule directory
        for (Location appSchedulesDir : schedulesBaseDir.list()) {
          if (!appSchedulesDir.isDirectory()) {
            continue;
          }
          String appName = appSchedulesDir.getName();

          for (Location scheduleFile : appSchedulesDir.list()) {
            if (!scheduleFile.getName().endsWith(".json")) {
              continue;
            }

            String scheduleName = scheduleFile.getName().replace(".json", "");
            try {
              ScheduleDetail detail;
              try (Reader reader = new InputStreamReader(scheduleFile.getInputStream(), StandardCharsets.UTF_8)) {
                detail = GSON.fromJson(reader, ScheduleDetail.class);
              }

              // Reconstruct the ProgramSchedule from the ScheduleDetail.
              ApplicationId appId = new ApplicationId(namespaceName, appName);
              ProgramType programType = ProgramType.valueOfSchedulableType(detail.getProgram().getProgramType());
              ProgramId programId = appId.program(programType, detail.getProgram().getProgramName());

              ProgramSchedule scheduleToImport = new ProgramSchedule(
                  scheduleName,
                  detail.getDescription(),
                  programId,
                  detail.getProperties(),
                  detail.getTrigger(),
                  (List<? extends Constraint>) detail.getConstraints(),
                  detail.getTimeoutMillis()
              );

              // Try to add, if it exists, update it.
              try {
                scheduler.addSchedule(scheduleToImport);
              } catch (AlreadyExistsException e) {
                LOG.info("Schedule '{}' in app '{}' already exists. Updating it.", scheduleName, appName);
                scheduler.updateSchedule(scheduleToImport);
              }

              // Re-enable the schedule if its status was SCHEDULED in the backup.
              if ("SCHEDULED".equalsIgnoreCase(detail.getStatus())) {
                scheduler.enableSchedule(scheduleToImport.getScheduleId());
              }

              report.addSuccess("Schedule", scheduleName, namespaceName, scheduleFile.toURI().toString());
              LOG.info("Successfully imported schedule '{}' for app '{}' in namespace '{}'",
                  scheduleName, appName, namespaceName);

            } catch (Exception e) {
              LOG.error("Failed to import schedule '{}' for app '{}' in namespace '{}'",
                  scheduleName, appName, namespaceName, e);
              report.addFailure("Schedule", scheduleName, namespaceName, e.getMessage());
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to process schedules for namespace '{}'", namespaceName, e);
        report.addFailure("Schedule-Batch", "ALL", namespaceName, "Failed to list schedules: " + e.getMessage());
      }
    }
    LOG.info("Finished importing schedules.");
  }

}

class SystemArtifactMetaInfo {
  private final Id.Artifact artifactId;
  private final Location jarLocation;
  private final Set<ArtifactRange> parents;
  private final Set<io.cdap.cdap.api.plugin.PluginClass> plugins;
  private final Set<io.cdap.cdap.api.artifact.ApplicationClass> apps; // <-- FIX: Add field for apps
  private final Map<String, String> properties;

  SystemArtifactMetaInfo(Id.Artifact artifactId, Location jarLocation, Set<ArtifactRange> parents,
      Set<io.cdap.cdap.api.plugin.PluginClass> plugins,
      Set<io.cdap.cdap.api.artifact.ApplicationClass> apps, // <-- FIX: Add apps to constructor
      Map<String, String> properties) {
    this.artifactId = artifactId;
    this.jarLocation = jarLocation;
    this.parents = parents;
    this.plugins = plugins;
    this.apps = apps; // <-- FIX: Assign apps
    this.properties = properties;
  }

  Id.Artifact getArtifactId() {
    return artifactId;
  }

  Location getJarLocation() {
    return jarLocation;
  }

  Set<ArtifactRange> getParents() {
    return parents;
  }

  Set<PluginClass> getPlugins() {
    return plugins;
  }

  Set<ApplicationClass> getApps() { // <-- FIX: Add getter for apps
    return apps;
  }

  Map<String, String> getProperties() {
    return properties;
  }

  public boolean hasParent(Id.Artifact artifactId) {
    if (parents == null) {
      return false;
    }
    for (ArtifactRange range : parents) {
      if (range.getName().equals(artifactId.getName()) && range.versionIsInRange(
          artifactId.getVersion())) {
        return true;
      }
    }
    return false;
  }
}

