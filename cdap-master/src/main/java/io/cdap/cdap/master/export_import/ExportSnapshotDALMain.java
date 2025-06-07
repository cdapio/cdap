package io.cdap.cdap.master.export_import;

import static io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO;

import com.google.api.client.util.Throwables;
import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.guice.AuditLogWriterModule;
import io.cdap.cdap.app.guice.NamespaceAdminModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.config.PreferencesTable;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactoryProvider;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.AuthorizationArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.LocalArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.schedule.DistributedTimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.RemoteScheduleManager;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleManager;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import io.cdap.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.store.TriggerMisfireLogger;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.profile.ProfileStore;
import io.cdap.cdap.internal.capability.CapabilityModule;
import io.cdap.cdap.internal.operation.guice.OperationModule;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;
import io.cdap.cdap.metadata.LocalPreferencesFetcherInternal;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
// import io.cdap.cdap.scheduler.CoreSchedulerService;
// import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.proto.profile.Profile;
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
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.sourcecontrol.guice.SourceControlModule;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.store.NamespaceTable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import com.google.inject.Guice;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
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
import com.google.inject.Injector;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.proto.NamespaceMeta;

/**
 * Proof-of-Concept for an atomic exporter.
 * All database read operations are wrapped in a single transaction to ensure
 * a consistent, point-in-time snapshot of the data is exported.
 */
public class ExportSnapshotDALMain {

  private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotDALMain.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
      new GsonBuilder().setPrettyPrinting()
  ).create();

  public static void main(String[] args) {
    long jobStartTime = System.currentTimeMillis();
    LOG.info("Export job started at {}", jobStartTime);
    LOG.debug("Received arguments: {}", Arrays.toString(args));

    if (args.length < 1) {
      LOG.error("Usage: ExportJobMain <gcs-bucket-uri>");
      LOG.error("Example: ExportJobMain gs://my-backup-bucket/run-123");
      System.exit(1);
    }

    JobReport report = new JobReport(JobReport.JobType.EXPORT);
    // Service secureStoreServiceHandle = null;
    // Service schedulerServiceHandle = null;
    // --- Timing and Counter Variables ---
    long setupEndTime = 0;
    AtomicLong namespaceExportStartTime = new AtomicLong();
    AtomicLong namespaceExportEndTime = new AtomicLong();
    AtomicLong pipelineExportStartTime = new AtomicLong();
    AtomicLong pipelineExportEndTime = new AtomicLong();
    long transactionStartTime = 0, transactionEndTime = 0;
    AtomicInteger namespaceCount = new AtomicInteger(0);
    AtomicInteger pipelineCount = new AtomicInteger(0);
    AtomicInteger scheduleCount = new AtomicInteger(0);
    AtomicInteger preferenceCount = new AtomicInteger(0);
    try {
      Injector injector = initializeInjector();

      // Start all necessary services before the transaction begins
      // SecureStoreService secureStoreService = injector.getInstance(SecureStoreService.class);
      // if (secureStoreService instanceof Service) {
      //   secureStoreServiceHandle = (Service) secureStoreService;
      //   secureStoreServiceHandle.startAndWait();
      // }

      // Scheduler schedulerService = injector.getInstance(Scheduler.class);
      // if (schedulerService instanceof Service) {
      //   schedulerServiceHandle = (Service) schedulerService;
      //   schedulerServiceHandle.startAndWait();
      // }

      CConfiguration cConf = injector.getInstance(CConfiguration.class);
      TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      String gcsBackupPath = "gs://" + args[0] + "/";
      report.open(locationFactory, gcsBackupPath);
      Location baseLocation = locationFactory.create(gcsBackupPath).append("cdap");
      baseLocation.mkdirs();
      setupEndTime = System.currentTimeMillis();
      // --- ATOMICITY POC ---
      // Wrap all database export operations in a single transaction.
      // This provides a consistent point-in-time view of the database.
      LOG.info("Starting atomic export transaction...");
      TransactionRunners.run(transactionRunner, context -> {
        LOG.info("Using TransactionRunner implementation: {}", transactionRunner.getClass().getName());
        // All export functions now receive the transaction 'context' directly
        // Note: Services like ArtifactStore and SecureStore manage their own transactions internally.
        // For a true atomic read, their underlying logic would need to be refactored to accept
        // an external transaction context. This POC demonstrates the principle with the classes
        // that can be easily adapted.
        LOG.info("Using this tx runner: {}", transactionRunner);
        namespaceExportStartTime.set(System.currentTimeMillis());
        List<NamespaceMeta> allNamespaces = getAllNamespaces(context);
        exportNamespaces(allNamespaces, baseLocation, report, namespaceCount);
        namespaceExportEndTime.set(System.currentTimeMillis());

        pipelineExportStartTime.set(System.currentTimeMillis());
        exportPipelinesSchedulesAndPrefs(allNamespaces, context, baseLocation, report, pipelineCount, scheduleCount,preferenceCount);
        pipelineExportEndTime.set(System.currentTimeMillis());
        // For Artifacts and SecureKeys, we'll get new instances that operate within this tx context
        // ArtifactStore artifactStore = new ArtifactStore(injector.getInstance(CConfiguration.class),
        //     injector.getInstance(io.cdap.cdap.common.namespace.NamespacePathLocator.class),
        //     injector.getInstance(LocationFactory.class),
        //     injector.getInstance(Impersonator.class),
        //     transactionRunner); // Use the main runner

        // SecureStore secureStore = (SecureStore)secureStoreService; // Use the started service
        //
        // exportUserPlugins(allNamespaces, context, artifactStore, baseLocation, report);
        // exportSystemPlugins(context, artifactStore, baseLocation, report);
        // exportSecureKeys(allNamespaces, secureStore, baseLocation, report);

        // long start = System.currentTimeMillis();
        // exportProfiles(context, allNamespaces, baseLocation, report, profileCounter);
        // profileExportTime = System.currentTimeMillis() - start;
      });
      transactionEndTime = System.currentTimeMillis();
      LOG.info("Atomic export transaction finished successfully.");

    } catch (Exception e) {
      LOG.error("Job failed with an unrecoverable error.", e);
      System.exit(1);
    } finally {
      // if (schedulerServiceHandle != null) {
      //   schedulerServiceHandle.stopAndWait();
      // }
      // if (secureStoreServiceHandle != null) {
      //   secureStoreServiceHandle.stopAndWait();
      // }
      report.close();
      long jobEndTime = System.currentTimeMillis();
      // --- LOG FINAL REPORT ---
      LOG.info("==================== EXPORT SUMMARY ====================");
      LOG.info(String.format("Total Setup Time:           %.2f seconds", (setupEndTime - jobStartTime) / 1000.0));
      LOG.info(String.format("Total Transaction Time:       %.2f seconds", (transactionEndTime - transactionStartTime) / 1000.0));
      LOG.info("------------------------------------------------------");
      LOG.info(String.format("Namespace Export Time:      %.2f seconds for %d namespaces",
          (namespaceExportEndTime.get() - namespaceExportStartTime.get()) / 1000.0, namespaceCount.get()));
      LOG.info(String.format("Pipeline & Schedule Export Time: %.2f seconds for %d pipelines and %d schedules and %d app preferences",
          (pipelineExportEndTime.get() - pipelineExportStartTime.get()) / 1000.0, pipelineCount.get(), scheduleCount.get(), preferenceCount.get()));
      LOG.info("------------------------------------------------------");
      LOG.info(String.format("Total Job Time:             %.2f seconds", (jobEndTime - jobStartTime) / 1000.0));
      LOG.info("========================================================");

      LOG.info("Job finished.");
    }
  }

  private static List<NamespaceMeta> getAllNamespaces(StructuredTableContext context)
      throws IOException {
    NamespaceTable namespaceTable = new NamespaceTable(context);
    return namespaceTable.list();
  }

  /**
   * Sets up the Guice injector with all necessary modules.
   */
  private static Injector initializeInjector() throws MalformedURLException {
    LOG.info("Initializing Guice injector...");

    CConfiguration cConf = CConfiguration.create();
    cConf.set("data.storage.properties.gcp-spanner.use.read.only.tx", "true");
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
        new TransactionExecutorModule(),
        coreSecurityModule,
        new StorageModule(),
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
            // bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
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
            bind(TransactionSystemClientService.class).to(
                DelegatingTransactionSystemClientService.class);
            bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
            bind(PluginFinder.class).to(LocalPluginFinder.class);
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


  public static void exportNamespaces(List<NamespaceMeta> namespaces, Location baseLocation, JobReport report,
      AtomicInteger namespaceCounter) {
    LOG.info("Starting export of namespaces...");
    try {
      Location namespacesDir = baseLocation.append("namespaces");
      namespacesDir.mkdirs();

      for (NamespaceMeta namespace : namespaces) {
        namespaceCounter.incrementAndGet();
        String namespaceId = namespace.getName();
        try {
          Location namespaceFile = namespacesDir.append(namespaceId).append("namespaceMeta.json");
          try (Writer writer = new OutputStreamWriter(namespaceFile.getOutputStream(), StandardCharsets.UTF_8)) {
            GSON.toJson(namespace, writer);
          }
          report.addSuccess("Namespace", namespaceId, "N/A", namespaceFile.toURI().toString());
        } catch (Exception e) {
          LOG.error("Failed to export namespace '{}'", namespaceId, e);
          report.addFailure("Namespace", namespaceId, "N/A", e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.error("An unrecoverable error occurred during namespace export", e);
      report.addFailure("Namespace-Batch", "ALL", "N/A", e.getMessage());
    }
    LOG.info("Finished exporting {} namespaces.", namespaceCounter.get());
  }

  public static void exportPipelinesSchedulesAndPrefs(List<NamespaceMeta> namespaces, StructuredTableContext context,
      Location baseLocation, JobReport report,
      AtomicInteger pipelineCounter, AtomicInteger scheduleCounter, AtomicInteger preferenceCounter) {
    LOG.info("Starting export of all pipeline versions and schedules within a single transaction...");
    final int batchSize = 1000;
    AppMetadataStore appStore = AppMetadataStore.create(context);
    ProgramScheduleStoreDataset scheduleStore = Schedulers.getScheduleStore(context);
    PreferencesTable preferencesTable = new PreferencesTable(context);

    for (NamespaceMeta namespace : namespaces) {
      NamespaceId namespaceId = new NamespaceId(namespace.getName());
      LOG.info("Starting pipeline and schedule export for namespace '{}'.", namespaceId.getNamespace());

      try {
        Location namespaceBackupLocation = baseLocation.append("namespaces").append(namespace.getName());
        Location pipelinesBackupLocation = namespaceBackupLocation.append("pipelines");

        AtomicReference<ApplicationId> lastAppId = new AtomicReference<>(null);
        AtomicBoolean moreToScan = new AtomicBoolean(true);
        Set<String> processedAppsForSchedules = new HashSet<>();

        while (moreToScan.get()) {
          AtomicInteger processedInBatch = new AtomicInteger(0);
          LOG.debug("Scanning for next batch of pipelines in namespace '{}' from: {}", namespaceId, lastAppId.get());
          ScanApplicationsRequest.Builder requestBuilder = ScanApplicationsRequest.builder()
              .setNamespaceId(namespaceId)
              .setLatestOnly(false)
              .setLimit(batchSize);

          if (lastAppId.get() != null) {
            requestBuilder.setScanFrom(lastAppId.get());
          }

          appStore.scanApplications(requestBuilder.build(), entry -> {
            lastAppId.set(entry.getKey());
            processedInBatch.incrementAndGet();

            ApplicationId appId = entry.getKey();
            String pipelineName = appId.getApplication();
            String pipelineVersion = appId.getVersion();
            pipelineCounter.incrementAndGet();
            LOG.debug("Processing pipeline: {} version {}", pipelineName, pipelineVersion);

            try {
              Location pipelineDir = pipelinesBackupLocation.append(pipelineName);
              Location pipelineVersionDir = pipelineDir.append(pipelineVersion);
              pipelineVersionDir.mkdirs();
              Location pipelineFile = pipelineVersionDir.append("pipeline.json");

              try (Writer writer = new OutputStreamWriter(pipelineFile.getOutputStream(), StandardCharsets.UTF_8)) {
                GSON.toJson(entry.getValue(), writer);
              }
              report.addSuccess("Pipeline", String.format("%s (v%s)", pipelineName, pipelineVersion),
                  namespace.getName(), pipelineFile.toURI().toString());
            } catch (Exception e) {
              LOG.error("Failed to export pipeline '{}/{}' in namespace '{}'",
                  pipelineName, pipelineVersion, namespace.getName(), e);
              report.addFailure("Pipeline", String.format("%s (v%s)", pipelineName, pipelineVersion),
                  namespace.getName(), e.getMessage());
            }

            if (processedAppsForSchedules.add(appId.getApplication())) {
              LOG.debug("First time seeing app '{}', exporting its schedules.", appId.getApplication());
              try {
                List<ProgramScheduleRecord> schedules = scheduleStore.listScheduleRecords(appId);
                LOG.debug("Found {} schedules for app '{}'", schedules.size(), appId.getApplication());
                if (!schedules.isEmpty()) {
                  Location appSchedulesDir = namespaceBackupLocation.append("schedules").append(pipelineName);
                  appSchedulesDir.mkdirs();
                  for (ProgramScheduleRecord record : schedules) {
                    scheduleCounter.incrementAndGet();
                    String scheduleName = record.getSchedule().getName();
                    try {
                      LOG.debug("Exporting schedule '{}'", scheduleName);
                      ScheduleDetail detail = record.toScheduleDetail();
                      Location scheduleFile = appSchedulesDir.append(scheduleName + ".json");
                      try (Writer writer = new OutputStreamWriter(scheduleFile.getOutputStream(), StandardCharsets.UTF_8)) {
                        GSON.toJson(detail, writer);
                      }
                      report.addSuccess("Schedule", scheduleName, namespace.getName(), scheduleFile.toURI().toString());
                    } catch (Exception e) {
                      LOG.error("Failed to export schedule '{}' for app '{}'", scheduleName, pipelineName, e);
                      report.addFailure("Schedule", scheduleName, namespace.getName(), e.getMessage());
                    }
                  }
                }
              } catch(Exception e) {
                LOG.error("Failed to export schedules for app '{}' in namespace '{}'", pipelineName, namespace.getName(), e);
                report.addFailure("Schedule-Batch", pipelineName, namespace.getName(), e.getMessage());
              }

              // --- EXPORT APPLICATION AND PROGRAM PREFERENCES ---
              LOG.debug("Exporting preferences for application '{}'", pipelineName);
              try {
                // Application preferences
                PreferencesDetail appPrefs = preferencesTable.getPreferences(appId);
                if (appPrefs != null && !appPrefs.getProperties().isEmpty()) {
                  preferenceCounter.incrementAndGet();
                  Location appPrefsDir = namespaceBackupLocation.append("preferences").append(pipelineName);
                  appPrefsDir.mkdirs();
                  Location appPrefsFile = appPrefsDir.append("preferences.json");
                  try (Writer writer = new OutputStreamWriter(appPrefsFile.getOutputStream(), StandardCharsets.UTF_8)) {
                    GSON.toJson(appPrefs.getProperties(), writer);
                  }
                  report.addSuccess("Preference-App", pipelineName, namespace.getName(), appPrefsFile.toURI().toString());
                }
              } catch (Exception e) {
                LOG.error("Failed to export preferences for application '{}'", pipelineName, e);
                report.addFailure("Preference-Batch", pipelineName, namespace.getName(), e.getMessage());
              }
            }
            return true;
          });

          if (processedInBatch.get() < batchSize) {
            moreToScan.set(false);
          }
        }
      } catch (Exception e) {
        LOG.error("An unrecoverable error occurred during pipeline and schedule export for namespace '{}'",
            namespace.getName(), e);
        report.addFailure("Pipeline-Batch", "ALL", namespace.getName(), e.getMessage());
      }
    }
    LOG.info("Finished exporting {} pipelines and their schedules.", pipelineCounter.get());
  }
  public static void exportUserPlugins(List<NamespaceMeta> namespaces, StructuredTableContext context,
      ArtifactStore artifactStore, Location baseLocation, JobReport report) {
    // In a real implementation, artifactStore.getArtifacts would need to accept the context.
    // For this POC, we'll assume it participates in the ongoing transaction.
    LOG.info("Starting export of user plugins...");
    // Logic for exporting user plugins...
  }

  public static void exportSystemPlugins(StructuredTableContext context, ArtifactStore artifactStore,
      Location baseLocation, JobReport report) {
    // Similar to user plugins...
    LOG.info("Starting export of system plugins...");
    // Logic for exporting system plugins...
  }

  public static void exportSecureKeys(List<NamespaceMeta> namespaces, SecureStore secureStore,
      Location baseLocation, JobReport report) {
    // SecureStore already manages its own transactions, but is called within the main block
    // for conceptual consistency in the POC.
    LOG.info("Starting export of secure keys...");
    // Logic for exporting secure keys...
  }

  /**
   * Exports all system and user-scoped compute profiles within a single transaction.
   *
   * @param context         the transaction context to use for database operations
   * @param allNamespaces   list of all user namespaces in the instance
   * @param baseLocation    the base GCS location for the export (e.g., gs://bucket/cdap/)
   * @param report          the job report for logging successes and failures
   * @param profileCounter  a counter for tracking the number of exported profiles
   */
  public static void exportProfiles(StructuredTableContext context, List<NamespaceMeta> allNamespaces,
      Location baseLocation, JobReport report, AtomicInteger profileCounter) {
    LOG.info("Starting export of compute profiles...");
    ProfileStore profileStore = ProfileStore.get(context);

    // Create a list of all namespaces to process, including the system namespace
    List<NamespaceId> namespacesToProcess = new ArrayList<>();
    namespacesToProcess.add(NamespaceId.SYSTEM);
    allNamespaces.forEach(meta -> namespacesToProcess.add(new NamespaceId(meta.getName())));

    for (NamespaceId namespaceId : namespacesToProcess) {
      String namespaceName = namespaceId.getNamespace();
      LOG.debug("Exporting profiles for namespace '{}'.", namespaceName);

      try {
        // Use the ProfileStore to get the list of profiles within the transaction
        List<Profile> profiles = profileStore.getProfiles(namespaceId, false);
        if (profiles.isEmpty()) {
          continue;
        }

        Location profilesDir = baseLocation.append("namespaces").append(namespaceName).append("profiles");
        profilesDir.mkdirs();

        for (Profile profile : profiles) {
          String profileName = profile.getName();
          try {
            profileCounter.incrementAndGet();
            Location profileFile = profilesDir.append(profileName + ".json");

            try (Writer writer = new OutputStreamWriter(profileFile.getOutputStream(), StandardCharsets.UTF_8)) {
              GSON.toJson(profile, writer);
            }

            report.addSuccess("Profile", profileName, namespaceName, profileFile.toURI().toString());
            LOG.debug("Successfully exported profile '{}' in namespace '{}'", profileName, namespaceName);

          } catch (Exception e) {
            LOG.error("Failed to export profile '{}' in namespace '{}'", profileName, namespaceName, e);
            report.addFailure("Profile", profileName, namespaceName, e.getMessage());
          }
        }
      } catch (Exception e) {
        LOG.error("An unrecoverable error occurred during profile export for namespace '{}'", namespaceName, e);
        report.addFailure("Profile-Batch", "ALL", namespaceName, "Failed to list profiles: " + e.getMessage());
      }
    }
    LOG.info("Finished exporting compute profiles.");
  }
}