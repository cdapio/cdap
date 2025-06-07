package io.cdap.cdap.master.export_import;

import static io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO;

import com.google.api.client.util.Throwables;
import com.google.common.util.concurrent.Service;
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
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.guice.AuditLogWriterModule;
import io.cdap.cdap.app.guice.NamespaceAdminModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
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
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
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
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
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
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.capability.CapabilityModule;
import io.cdap.cdap.internal.operation.guice.OperationModule;
import io.cdap.cdap.internal.pipeline.SynchronousPipelineFactory;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.messaging.guice.MessagingServiceModule;
import io.cdap.cdap.metadata.LocalPreferencesFetcherInternal;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.pipeline.PipelineFactory;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.runtime.spi.provisioner.dataproc.ComputeEngineCredentials;
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
import io.cdap.cdap.store.NamespaceTable;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import com.google.inject.Guice;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.base.Supplier;
import javax.net.ssl.HttpsURLConnection;
import org.apache.curator.shaded.com.google.common.io.ByteStreams;
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
import io.cdap.cdap.common.conf.CConfiguration;
import com.google.inject.Injector;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.proto.NamespaceMeta;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.GenericData;
import com.google.auth.oauth2.AccessToken;
import com.google.common.io.CharStreams;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


public class ExporterMainNonHTTP {

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private static final Logger LOG = (Logger) LoggerFactory.getLogger(ExporterMain.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
      new GsonBuilder().setPrettyPrinting()
  ).create();

  public static void main(String[] args) throws IOException {
    long jobStartTime = System.currentTimeMillis();
    LOG.debug("Received arguments: {}", Arrays.toString(args));
    String p4saEmail = args[3];

    if (args.length < 1) {
      LOG.error("Usage: ExportJobMain <gcs-bucket-uri>");
      LOG.error("Example: ExportJobMain gs://my-backup-bucket/run-123");
      System.exit(1);
    }

    JobReport report = new JobReport(JobReport.JobType.EXPORT);
    Service secureStoreServiceHandle = null;
    Scheduler schedulerService = null;
    // --- Timing and Counter Variables ---
    long setupTime = 0;
    long namespaceTime = 0, pipelineTime = 0, systemPluginTime = 0, userPluginTime = 0, secureKeyTime = 0;
    AtomicInteger namespaceCount = new AtomicInteger(0);
    AtomicInteger pipelineCount = new AtomicInteger(0);
    AtomicInteger scheduleCount = new AtomicInteger(0);
    AtomicInteger systemPluginCount = new AtomicInteger(0);
    AtomicInteger userPluginCount = new AtomicInteger(0);
    AtomicInteger secureKeyCount = new AtomicInteger(0);
    AtomicInteger preferenceCount = new AtomicInteger(0);

    try {
      long startTime = System.currentTimeMillis();
      Injector injector = initializeInjector(p4saEmail, args[1]);
      // SecureStoreService secureStoreService = injector.getInstance(SecureStoreService.class);
      // if (secureStoreService instanceof Service) {
      //   secureStoreServiceHandle = (Service) secureStoreService;
      //   secureStoreServiceHandle.startAndWait();
      // }
      // schedulerService = injector.getInstance(Scheduler.class);
      // if (schedulerService instanceof Service) {
      //   ((Service) schedulerService).startAndWait();
      // }

      TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      // ArtifactStore artifactStore = injector.getInstance(ArtifactStore.class);
      // SecureStore secureStore = secureStoreService;

      String gcsBackupPath = "gs://" + args[0] + "/";
      report.open(locationFactory, gcsBackupPath);
      String tokenEndpoint =
          "https://cdap-" + args[1] + "-task-worker:11020/v3Internal/worker/token";
      int maxRetries = 3;
      // ComputeEngineCredentials credentials = ComputeEngineCredentials.getOrCreate(tokenEndpoint,
      //     maxRetries);
      // AccessToken accessToken = credentials.refreshAccessToken();
      AccessToken accessToken = getAccessTokenRemotely(tokenEndpoint);

      if (accessToken != null) {
        System.out.println("\n--- POC SUCCESS ---");
        System.out.println("Successfully obtained access token!");
        System.out.println(
            "Token Value (first 15 chars): " + accessToken.getTokenValue().substring(0, 15)
                + "...");
        System.out.println("Token Expiration: " + accessToken.getExpirationTime());
        System.out.println("--------------------");
      } else {
        System.err.println("\n--- POC FAILURE ---");
        System.err.println(
            "Failed to obtain access token. The credentials object returned a null token.");
        System.err.println("--------------------");
      }
      // ---------------
      // Export Namespaces
      startTime = System.currentTimeMillis();
      List<NamespaceMeta> allNamespaces = getAllNamespaces(transactionRunner);
      exportNamespaces(allNamespaces, locationFactory, gcsBackupPath, report, namespaceCount);
      namespaceTime = System.currentTimeMillis() - startTime;

      // export pipelines
      startTime = System.currentTimeMillis();
      // exportPipelinesAndSchedules(allNamespaces, transactionRunner, locationFactory, gcsBackupPath,
      //     report, schedulerService, pipelineCount, scheduleCount);
      exportPipelinesSchedulesAndPreferences(allNamespaces, transactionRunner, locationFactory, gcsBackupPath,
          report, pipelineCount, scheduleCount, preferenceCount);
      pipelineTime = System.currentTimeMillis() - startTime;
      // --------------
      // Export system plugins
      // startTime = System.currentTimeMillis();
      // exportSystemPlugins(artifactStore, locationFactory, gcsBackupPath, report);
      // systemPluginTime = System.currentTimeMillis() - startTime;

      // export user plugins
      // startTime = System.currentTimeMillis();
      // exportUserPlugins(allNamespaces, locationFactory, artifactStore, gcsBackupPath, report);
      // userPluginTime = System.currentTimeMillis() - startTime;

      // Secure keys
      // startTime = System.currentTimeMillis();
      // exportSecureKeys(allNamespaces, secureStore, locationFactory, gcsBackupPath, report);
      // secureKeyTime = System.currentTimeMillis() - startTime;

      // ... after exporting namespaces and plugins ...
      // long startTime = System.currentTimeMillis();
      // exportProfiles(allNamespaces, locationFactory, profileService, gcsBackupPath, report, profileCounter);
      // profileExportTime = System.currentTimeMillis() - startTime;
      LOG.info("Job finished successfully.");
    } catch (Exception e) {
      LOG.error("Job failed with an unrecoverable error.", e);
      System.exit(1);
    } finally {
      // if (schedulerService instanceof Service) {
      //   ((Service) schedulerService).stopAndWait();
      // }
      // if (secureStoreServiceHandle != null) {
      //   secureStoreServiceHandle.stopAndWait();
      // }
      long jobEndTime = System.currentTimeMillis();
      // --- NEW: Gather summary data and write it to the summary report file ---
      if (report.isOpen()) {
        java.util.Map<String, String> summaryData = new java.util.LinkedHashMap<>();
        summaryData.put("Total Job Time (seconds)",
            String.format("%.2f", (jobEndTime - jobStartTime) / 1000.0));
        summaryData.put("Total Setup Time (seconds)", String.format("%.2f", setupTime / 1000.0));
        summaryData.put("---", "---"); // Separator for clarity
        summaryData.put("Namespace Export Time (seconds)",
            String.format("%.2f", namespaceTime / 1000.0));
        summaryData.put("Namespace Count", namespaceCount.toString());
        summaryData.put("Pipeline, Schedule & Preference Export Time (seconds)",
            String.format("%.2f", pipelineTime / 1000.0));
        summaryData.put("Pipeline Version Count", pipelineCount.toString());
        summaryData.put("Schedule Count", scheduleCount.toString());
        summaryData.put("Preference Set Count", preferenceCount.toString());
        summaryData.put("System Plugin Export Time (seconds)",
            String.format("%.2f", systemPluginTime / 1000.0));
        summaryData.put("System Plugin Count", systemPluginCount.toString());
        summaryData.put("User Plugin Export Time (seconds)",
            String.format("%.2f", userPluginTime / 1000.0));
        summaryData.put("User Plugin Count", userPluginCount.toString());
        summaryData.put("Secure Key Export Time (seconds)",
            String.format("%.2f", secureKeyTime / 1000.0));
        summaryData.put("Secure Key Count", secureKeyCount.toString());

        report.writeSummaryReport(summaryData);
      }
      report.close();
      // --- LOG FINAL REPORT ---
      LOG.info("==================== EXPORT SUMMARY ====================");
      LOG.info(String.format("Total Setup Time:                %.2f seconds", setupTime / 1000.0));
      LOG.info("------------------------------------------------------");
      LOG.info(String.format("Namespace Export:           %.2f seconds for %d namespaces",
          namespaceTime / 1000.0, namespaceCount.get()));
      LOG.info(String.format(
          "Pipeline & Schedule Export: %.2f seconds for %d pipelines and %d schedules and %d pipelines",
          pipelineTime / 1000.0, pipelineCount.get(), scheduleCount.get(), preferenceCount.get()));
      LOG.info(String.format("System Plugin Export:       %.2f seconds for %d plugins",
          systemPluginTime / 1000.0, systemPluginCount.get()));
      LOG.info(String.format("User Plugin Export:         %.2f seconds for %d plugins",
          userPluginTime / 1000.0, userPluginCount.get()));
      LOG.info(String.format("Secure Key Export:          %.2f seconds for %d keys",
          secureKeyTime / 1000.0, secureKeyCount.get()));
      LOG.info("------------------------------------------------------");
      LOG.info(String.format("Total Job Time:                  %.2f seconds",
          (jobEndTime - jobStartTime) / 1000.0));
      LOG.info("========================================================");

      LOG.info("Job finished.");
      LOG.info("Job finished.");
    }
  }

  public static AccessToken getAccessTokenRemotely(String endPoint) throws IOException {
    URL url = new URL(endPoint);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    if (connection instanceof HttpsURLConnection) {
      // TODO (CDAP-18047) enable ssl verification
      disableVerifySsl(((HttpsURLConnection) connection));
    }
    connection.connect();
    try (Reader reader = new InputStreamReader(connection.getInputStream(),
        StandardCharsets.UTF_8)) {
      if (connection.getResponseCode() != HttpResponseStatus.OK.code()) {
        throw new IOException(CharStreams.toString(reader));
      }
      GenericData token = GSON.fromJson(reader, GenericData.class);

      String ACCESS_TOKEN_KEY = "access_token";
      String EXPIRES_IN_KEY = "expires_in";

      if (!token.containsKey(ACCESS_TOKEN_KEY) || !token.containsKey(EXPIRES_IN_KEY)) {
        throw new IOException("Received invalid token");
      }

      String key = token.get(ACCESS_TOKEN_KEY).toString();
      Double expiration = Double.parseDouble(token.get(EXPIRES_IN_KEY).toString());
      long expiresAtMilliseconds = System.currentTimeMillis()
          + expiration.longValue() * 1000;

      LOG.info("GOT THE KEY!!! :{}", key);
      LOG.info("expiration date: {}", new Date(expiresAtMilliseconds));

      return new AccessToken(key, new Date(expiresAtMilliseconds));
    } finally {
      connection.disconnect();
    }
  }

   public static void disableVerifySsl(HttpsURLConnection connection) throws IOException {
    try {
      SSLContext sslContextWithNoVerify = SSLContext.getInstance("SSL");
      TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
          // No-op
        }

        @Override
        public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
          // No-op
        }
      }};
      sslContextWithNoVerify.init(null, trustAllCerts, SECURE_RANDOM);
      connection.setSSLSocketFactory(sslContextWithNoVerify.getSocketFactory());
      connection.setHostnameVerifier((s, sslSession) -> true);
    } catch (Exception e) {
      LOG.error("Unable to initialize SSL context", e);
      throw new IOException(e.getMessage());
    }
  }


  private static List<NamespaceMeta> getAllNamespaces(TransactionRunner transactionRunner) {
    List<NamespaceMeta> namespaces = TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable namespaceTable = new NamespaceTable(context);
      return namespaceTable.list();
    });
    return namespaces;
  }

  /**
   * Sets up the Guice injector with all necessary modules.
   */
  private static Injector initializeInjector(String p4saEmail, String instanceId)
      throws MalformedURLException {
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
    // hConf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    // hConf.set("fs.gs.auth.access.token.provider", "io.cdap.cdap.master.export_import.CdapTokenProviderAdapter");
    // String taskWorkerEdnpoint = "https://cdap-" + instanceId +"-task-worker:11020/v3Internal/worker/token";
    // hConf.set("gcs.auth.cdap.token.endpoint", taskWorkerEdnpoint);
    // The following property is needed to enable service account authentication via Workload Identity.
    // hConf.setBoolean("fs.gs.auth.service.account.enable", true);
    // hConf.set("fs.gs.auth.impersonation.service.account", p4saEmail);
    hConf.set("fs.gs.trace.logging.enable", "true");
    // hConf.set("google.cloud.auth.service.account.json.keyfile", "/etc/gcs-credentials/credentials.json");

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

  /**
   * Fetches all namespaces and uploads their metadata to a GCS location.
   */
  public static void exportNamespaces(List<NamespaceMeta> namespaces,
      LocationFactory locationFactory, String backupPath, JobReport report,
      AtomicInteger namespaceCounter) throws Exception {
    LOG.info("Starting export of namespaces to base path: {}", backupPath);
    Location baseLocation = locationFactory.create(backupPath);
    baseLocation.mkdirs();

    LOG.info("Found {} namespaces to export.", namespaces.size());
    Location namespacesDir = baseLocation.append("namespaces");

    for (NamespaceMeta namespace : namespaces) {
      namespaceCounter.incrementAndGet();
      String namespaceId = namespace.getName();
      try {
        LOG.debug("Processing namespace '{}'...", namespaceId);
        Location namespaceLocation = namespacesDir.append(namespaceId).append("namespaceMeta.json");
        try (OutputStream outputStream = namespaceLocation.getOutputStream();
            Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
          GSON.toJson(namespace, writer);
        }

        LOG.info("Successfully exported namespace '{}' to {}", namespace,
            namespaceLocation.toURI());
        report.addSuccess("Namespace", namespaceId, "N/A", namespaceLocation.toURI().toString());
      } catch (Exception e) {
        LOG.error("Failed to export namespace '{}'", namespaceId, e);
        report.addFailure("Namespace", namespaceId, "N/A", e.getMessage());
      }
    }

    LOG.info("Finished exporting all namespaces.");
  }

  /**
   * Exports all versions of all pipelines, their associated schedules, and their preferences from
   * all namespaces. This method uses a memory-efficient pagination strategy to handle large numbers
   * of pipelines. Each step is performed in its own transaction.
   *
   * @param allNamespaces list of all namespaces to export from
   * @param transactionRunner the transaction runner for database access
   * @param locationFactory factory for creating GCS locations
   * @param backupPath the base GCS path for the export
   * @param report the job report for logging successes and failures
   * @param pipelineCounter a counter for tracking the number of exported pipelines
   * @param scheduleCounter a counter for tracking the number of exported schedules
   * @param preferenceCounter a counter for tracking the number of exported preference sets
   */
  public static void exportPipelinesSchedulesAndPreferences(List<NamespaceMeta> allNamespaces,
      TransactionRunner transactionRunner,
      LocationFactory locationFactory, String backupPath,
      JobReport report,
      AtomicInteger pipelineCounter, AtomicInteger scheduleCounter,
      AtomicInteger preferenceCounter) {
    LOG.info("Starting export of all pipelines, schedules, and preferences...");
    final int batchSize = 20;

    for (NamespaceMeta namespace : allNamespaces) {
      NamespaceId namespaceId = new NamespaceId(namespace.getName());
      LOG.info("Starting export for namespace '{}'.", namespaceId.getNamespace());
      Set<String> processedApps = new HashSet<>();

      try {
        Location namespaceBackupLocation = locationFactory.create(backupPath).append("namespaces")
            .append(namespace.getName());
        Location pipelinesBackupLocation = namespaceBackupLocation.append("pipelines");
        Location schedulesBackupLocation = namespaceBackupLocation.append("schedules");

        AtomicReference<ApplicationId> lastAppId = new AtomicReference<>(null);
        boolean moreToScan = true;

        while (moreToScan) {
          AtomicInteger processedInBatch = new AtomicInteger(0);

          TransactionRunners.run(transactionRunner, context -> {
            AppMetadataStore appStore = AppMetadataStore.create(context);
            ScanApplicationsRequest request = ScanApplicationsRequest.builder()
                .setNamespaceId(namespaceId)
                .setLatestOnly(false)
                .setLimit(batchSize)
                .setScanFrom(lastAppId.get())
                .build();

            LOG.debug("Scanning for next batch of pipelines in namespace '{}' from: {}",
                namespaceId, lastAppId.get());

            appStore.scanApplications(request, entry -> {
              processedInBatch.incrementAndGet();
              pipelineCounter.incrementAndGet();

              ApplicationId appId = entry.getKey();
              ApplicationMeta appMeta = entry.getValue();
              lastAppId.set(appId);
              String pipelineName = appId.getApplication();
              String pipelineVersion = appId.getVersion();

              // Export Pipeline
              try {
                Location pipelineDir = pipelinesBackupLocation.append(pipelineName);
                Location pipelineVersionDir = pipelineDir.append(pipelineVersion);
                pipelineVersionDir.mkdirs();
                Location pipelineFile = pipelineVersionDir.append("pipeline.json");
                try (Writer writer = new OutputStreamWriter(pipelineFile.getOutputStream(),
                    StandardCharsets.UTF_8)) {
                  GSON.toJson(appMeta, writer);
                }
                report.addSuccess("Pipeline",
                    String.format("%s (v%s)", pipelineName, pipelineVersion),
                    namespace.getName(), pipelineFile.toURI().toString());
              } catch (Exception e) {
                LOG.error("Failed to export pipeline '{}/{}'", pipelineName, pipelineVersion, e);
                report.addFailure("Pipeline",
                    String.format("%s (v%s)", pipelineName, pipelineVersion),
                    namespace.getName(), e.getMessage());
              }

              // Export Schedules and Preferences only once per application name
              if (processedApps.add(pipelineName)) {
                exportSchedulesForApp(appId, transactionRunner, schedulesBackupLocation, report,
                    scheduleCounter, namespace.getName());
                exportPreferencesForApp(appId, appMeta, transactionRunner, namespaceBackupLocation,
                    report,
                    preferenceCounter, namespace.getName());
              }
              // Return true to keep scanning within this transaction until the limit is reached.
              return true;
            });
          });

          if (processedInBatch.get() < batchSize) {
            moreToScan = false;
          }
        }
      } catch (Exception e) {
        LOG.error("An unrecoverable error occurred during export for namespace '{}'",
            namespace.getName(), e);
        report.addFailure("Export-Batch", "ALL", namespace.getName(), e.getMessage());
      }
    }
  }

  private static void exportSchedulesForApp(ApplicationId appId,
      TransactionRunner transactionRunner,
      Location schedulesBackupLocation,
      JobReport report, AtomicInteger scheduleCounter, String namespaceName) {
    String appName = appId.getApplication();
    LOG.debug("Exporting schedules for app '{}'", appName);
    try {
      List<ProgramScheduleRecord> schedules = TransactionRunners.run(transactionRunner, context -> {
        ProgramScheduleStoreDataset scheduleStore = Schedulers.getScheduleStore(context);
        return scheduleStore.listScheduleRecords(appId);
      });

      if (!schedules.isEmpty()) {
        Location appSchedulesDir = schedulesBackupLocation.append(appName);
        appSchedulesDir.mkdirs();
        for (ProgramScheduleRecord record : schedules) {
          scheduleCounter.incrementAndGet();
          String scheduleName = record.getSchedule().getName();
          try {
            ScheduleDetail detail = record.toScheduleDetail();
            Location scheduleFile = appSchedulesDir.append(detail.getName() + ".json");
            try (Writer writer = new OutputStreamWriter(scheduleFile.getOutputStream(),
                StandardCharsets.UTF_8)) {
              GSON.toJson(detail, writer);
            }
            report.addSuccess("Schedule", detail.getName(), namespaceName,
                scheduleFile.toURI().toString());
          } catch (Exception e) {
            LOG.error("Failed to export schedule '{}' for app '{}'", scheduleName, appName, e);
            report.addFailure("Schedule", scheduleName, namespaceName, e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to list schedules for app '{}'", appName, e);
      report.addFailure("Schedule-Batch", appName, namespaceName, e.getMessage());
    }
  }

  private static void exportPreferencesForApp(ApplicationId appId, ApplicationMeta appMeta,
      TransactionRunner transactionRunner, Location namespaceBackupLocation,
      JobReport report, AtomicInteger preferenceCounter, String namespaceName) {
    LOG.debug("Exporting preferences for application '{}'", appId.getApplication());
    try {
      // Application-level preferences
      Map<String, String> appPrefs = TransactionRunners.run(transactionRunner, context -> {
        PreferencesTable preferencesTable = new PreferencesTable(context);
        return preferencesTable.getPreferences(appId).getProperties();
      });

      if (!appPrefs.isEmpty()) {
        preferenceCounter.incrementAndGet();
        Location appPrefsDir = namespaceBackupLocation.append("preferences")
            .append(appId.getApplication());
        appPrefsDir.mkdirs();
        Location appPrefsFile = appPrefsDir.append("preferences.json");
        try (Writer writer = new OutputStreamWriter(appPrefsFile.getOutputStream(),
            StandardCharsets.UTF_8)) {
          GSON.toJson(appPrefs, writer);
        }
        report.addSuccess("Preference-App", appId.getApplication(), namespaceName,
            appPrefsFile.toURI().toString());
      }
    } catch (Exception e) {
      LOG.error("Failed to export preferences for app '{}'", appId.getApplication(), e);
      report.addFailure("Preference-Batch", appId.getApplication(), namespaceName, e.getMessage());
    }
  }


  /**
   * Exports all versions of all pipelines from all namespaces using a memory-efficient pagination
   * strategy.
   */
  public static void exportPipelinesAndSchedules(List<NamespaceMeta> namespaces,
      TransactionRunner transactionRunner,
      LocationFactory locationFactory, String backupPath, JobReport report, Scheduler scheduler,
      AtomicInteger pipelineCounter, AtomicInteger scheduleCounter) {
    LOG.info("Starting export of all pipeline versions...");
    final int batchSize = 2; // Process 1000 pipelines per transaction to keep memory usage low.

    for (NamespaceMeta namespace : namespaces) {
      NamespaceId namespaceId = new NamespaceId(namespace.getName());
      LOG.info("Starting pipeline export for namespace '{}'.", namespaceId.getNamespace());

      try {
        Location namespaceBackupLocation = locationFactory.create(backupPath).append("namespaces")
            .append(namespace.getName());
        Location pipelinesBackupLocation = namespaceBackupLocation.append("pipelines");
        Location schedulesBackupLocation = namespaceBackupLocation.append("schedules");

        AtomicReference<ApplicationId> lastAppId = new AtomicReference<>(null);
        AtomicBoolean moreToScan = new AtomicBoolean(true);

        while (moreToScan.get()) {
          AtomicInteger processedInBatch = new AtomicInteger(0);

          TransactionRunners.run(transactionRunner, context -> {
            AppMetadataStore appStore = AppMetadataStore.create(context);

            ScanApplicationsRequest.Builder requestBuilder = ScanApplicationsRequest.builder()
                .setNamespaceId(namespaceId)
                .setLatestOnly(false) // CRITICAL: This ensures we get ALL versions.
                .setLimit(batchSize);

            if (lastAppId.get() != null) {
              requestBuilder.setScanFrom(lastAppId.get());
            }
            LOG.info("Starting scanning of applications...");
            // This scan method has a void return type. We control the scan inside the lambda.
            appStore.scanApplications(requestBuilder.build(), entry -> {
              pipelineCounter.incrementAndGet();
              ApplicationId appId = entry.getKey();
              ApplicationMeta appMeta = entry.getValue();
              LOG.info("Processing appId: {}", appId);

              lastAppId.set(appId);
              processedInBatch.incrementAndGet();

              String pipelineName = appId.getApplication();
              String pipelineVersion = appId.getVersion();

              try {
                Location pipelineDir = pipelinesBackupLocation.append(pipelineName);
                Location pipelineVersionDir = pipelineDir.append(pipelineVersion);
                pipelineVersionDir.mkdirs();

                Location pipelineFile = pipelineVersionDir.append("pipeline.json");

                try (OutputStream outputStream = pipelineFile.getOutputStream();
                    Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
                  GSON.toJson(appMeta, writer);
                }
                report.addSuccess("Pipeline",
                    String.format("%s (v%s)", pipelineName, pipelineVersion),
                    namespace.getName(), pipelineFile.toURI().toString());
              } catch (Exception e) {
                LOG.error("Failed to export pipeline '{}/{}' version '{}' in namespace '{}'",
                    pipelineName, pipelineVersion, namespace.getName(), e);
                report.addFailure("Pipeline",
                    String.format("%s (v%s)", pipelineName, pipelineVersion),
                    namespace.getName(), e.getMessage());
              }
              // --- EXPORT SCHEDULES FOR THIS APP (ONLY ONCE PER APP, NOT PER VERSION) ---
              // Since we get all versions, we only need to export schedules for the first version we see for an app.
              // A simple check can be if a directory for this app's schedules already exists.
              try {
                Location appSchedulesDir = schedulesBackupLocation.append(pipelineName);
                if (!appSchedulesDir.exists()) {
                  scheduleCounter.incrementAndGet();
                  appSchedulesDir.mkdirs();
                  List<ProgramSchedule> schedules = scheduler.listSchedules(appId);
                  for (ProgramSchedule schedule : schedules) {
                    String scheduleName = schedule.getName();
                    try {
                      ProgramScheduleRecord record = scheduler.getScheduleRecord(
                          schedule.getScheduleId());
                      ScheduleDetail detail = record.toScheduleDetail();
                      Location scheduleFile = appSchedulesDir.append(scheduleName + ".json");
                      try (Writer writer = new OutputStreamWriter(scheduleFile.getOutputStream(),
                          StandardCharsets.UTF_8)) {
                        GSON.toJson(detail, writer);
                      }
                      report.addSuccess("Schedule", scheduleName, namespace.getName(),
                          scheduleFile.toURI().toString());
                    } catch (Exception e) {
                      LOG.error("Failed to export schedule '{}' for app '{}'", scheduleName,
                          pipelineName, e);
                      report.addFailure("Schedule", scheduleName, namespace.getName(),
                          e.getMessage());
                    }
                  }
                }
              } catch (Exception e) {
                LOG.error("Failed to export schedules for app '{}' in namespace '{}'", pipelineName,
                    namespace.getName(), e);
                report.addFailure("Schedule-Batch", pipelineName, namespace.getName(),
                    e.getMessage());
              }

              // Return true to keep scanning within this transaction until the limit is reached.
              return true;
            });
          });

          // If the number of processed items is less than our batch size, we have reached the end.
          if (processedInBatch.get() < batchSize) {
            moreToScan.set(false);
          }
        }
      } catch (Exception e) {
        LOG.error("An unrecoverable error occurred during pipeline export for namespace '{}'",
            namespace.getName(), e);
        report.addFailure("Pipeline-Batch", "ALL", namespace.getName(), e.getMessage());
      }
    }
    LOG.info("Finished exporting all pipeline versions.");
  }


  /**
   * Exports all user plugins (artifacts) from all namespaces by using the ArtifactStore.
   */
  public static void exportUserPlugins(List<NamespaceMeta> namespaces,
      LocationFactory locationFactory, ArtifactStore artifactStore,
      String backupPath, JobReport report, AtomicInteger userPluginCounter) {
    LOG.info("Starting export of user plugins...");

    for (NamespaceMeta namespace : namespaces) {
      NamespaceId namespaceId = new NamespaceId(namespace.getName());
      LOG.info("Starting user plugin export for namespace '{}'.", namespaceId.getNamespace());

      try {
        Location namespaceBackupLocation = locationFactory.create(backupPath).append("namespaces")
            .append(namespace.getName());
        Location pluginsBackupDir = namespaceBackupLocation.append("plugins");
        pluginsBackupDir.mkdirs();

        // Use the ArtifactStore directly to get the list of artifacts
        List<ArtifactDetail> artifacts = artifactStore.getArtifacts(namespaceId);

        for (ArtifactDetail detail : artifacts) {
          userPluginCounter.incrementAndGet();
          String artifactName = detail.getDescriptor().getArtifactId().getName();
          String artifactVersion = String.valueOf(
              detail.getDescriptor().getArtifactId().getVersion());
          String artifactIdStr = String.format("%s:%s", artifactName, artifactVersion);

          try {
            Location artifactDir = pluginsBackupDir.append(artifactName).append(artifactVersion);
            artifactDir.mkdirs();

            // 1. Export the artifact metadata (ArtifactDetail) to meta.json
            Location metaFile = artifactDir.append("meta.json");
            try (Writer writer = new OutputStreamWriter(metaFile.getOutputStream(),
                StandardCharsets.UTF_8)) {
              GSON.toJson(detail, writer);
            }

            // 2. Export the artifact binary (e.g., the JAR file)
            Location sourceJarLocation = detail.getDescriptor().getLocation();
            if (sourceJarLocation == null || !sourceJarLocation.exists()) {
              throw new IOException(
                  "Artifact location does not exist or is not accessible: " + sourceJarLocation);
            }
            LOG.debug("sourceJarLocation for {} is {}", artifactName, sourceJarLocation);
            String originalJarName = sourceJarLocation.getName();
            Location jarFile = artifactDir.append(originalJarName);

            try (InputStream in = sourceJarLocation.getInputStream();
                OutputStream out = jarFile.getOutputStream()) {
              ByteStreams.copy(in, out);
            }

            report.addSuccess("Plugin", artifactIdStr, namespaceId.getNamespace(),
                jarFile.toURI().toString());

          } catch (Exception e) {
            LOG.error("Failed to export plugin '{}' in namespace '{}'", artifactIdStr,
                namespaceId.getNamespace(), e);
            report.addFailure("Plugin", artifactIdStr, namespaceId.getNamespace(), e.getMessage());
          }
        }

      } catch (Exception e) {
        LOG.error("An unrecoverable error occurred during user plugin export for namespace '{}'",
            namespaceId.getNamespace(), e);
        report.addFailure("Plugin-Batch", "ALL", namespaceId.getNamespace(),
            "Failed to list artifacts: " + e.getMessage());
      }
    }
    LOG.info("Finished exporting user plugins.");
  }

  public static void exportSystemPlugins(ArtifactStore artifactStore,
      LocationFactory locationFactory, String backupPath, JobReport report,
      AtomicInteger systemPluginCounter) {
    LOG.info("Starting export of system plugins...");
    try {
      Location baseLocation = locationFactory.create(backupPath).append("namespaces");

      List<ArtifactDetail> artifacts = artifactStore.getArtifacts(NamespaceId.SYSTEM);
      if (artifacts.isEmpty()) {
        LOG.info("No system plugins found to export.");
        return;
      }

      Location pluginsBackupDir = baseLocation.append("system").append("plugins");
      LOG.info("Found {} system plugins to export.", artifacts.size());

      for (ArtifactDetail detail : artifacts) {
        systemPluginCounter.incrementAndGet();
        String artifactName = detail.getDescriptor().getArtifactId().getName();
        String artifactVersion = detail.getDescriptor().getArtifactId().getVersion().toString();
        String artifactIdStr = String.format("%s-%s", artifactName, artifactVersion);
        try {
          Location artifactVersionDir = pluginsBackupDir.append(artifactName)
              .append(artifactVersion);
          artifactVersionDir.mkdirs();

          // Write metadata file
          Location metaFile = artifactVersionDir.append("meta.json");
          try (Writer writer = new OutputStreamWriter(metaFile.getOutputStream(),
              StandardCharsets.UTF_8)) {
            GSON.toJson(detail, writer);
          }

          // Write JAR file
          Location jarLocation = detail.getDescriptor().getLocation();
          Location destJarLocation = artifactVersionDir.append(jarLocation.getName());
          try (InputStream in = jarLocation.getInputStream();
              OutputStream out = destJarLocation.getOutputStream()) {
            ByteStreams.copy(in, out);
          }

          report.addSuccess("System-Plugin", artifactIdStr, "system",
              destJarLocation.toURI().toString());
        } catch (Exception e) {
          LOG.error("Failed to export system plugin '{}'", artifactIdStr, e);
          report.addFailure("System-Plugin", artifactIdStr, "system", e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to export system plugins.", e);
      report.addFailure("System-Plugin-Batch", "ALL", "system", e.getMessage());
    }
    LOG.info("Finished exporting system plugins.");
  }

  public static void exportSecureKeys(List<NamespaceMeta> namespaces, SecureStore secureStore,
      LocationFactory locationFactory, String backupPath, JobReport report,
      AtomicInteger secureKeyCounter) {
    LOG.info("Starting export of secure keys...");

    for (NamespaceMeta namespace : namespaces) {
      String namespaceName = namespace.getName();
      LOG.info("Starting secure key export for namespace '{}'.", namespaceName);

      try {
        Location secureKeysDir = locationFactory.create(backupPath).append("namespaces")
            .append(namespaceName).append("securekeys");
        secureKeysDir.mkdirs();

        Collection<SecureStoreMetadata> allMetadata = secureStore.list(namespaceName);

        for (SecureStoreMetadata metadata : allMetadata) {
          secureKeyCounter.incrementAndGet();
          String keyName = metadata.getName();
          try {
            // Get the secret, which contains the DECRYPTED data
            SecureStoreData secretData = secureStore.get(namespaceName, keyName);

            // Create a DTO for safe export, encoding the decrypted bytes as Base64
            SecureKeyExportDTO exportDTO = new SecureKeyExportDTO();
            exportDTO.setMetadata(metadata);
            exportDTO.setDataB64(Base64.getEncoder().encodeToString(secretData.get()));

            Location keyFile = secureKeysDir.append(keyName + ".json");
            try (Writer writer = new OutputStreamWriter(keyFile.getOutputStream(),
                StandardCharsets.UTF_8)) {
              GSON.toJson(exportDTO, writer);
            }
            report.addSuccess("SecureKey", keyName, namespaceName, keyFile.toURI().toString());

          } catch (Exception e) {
            LOG.error("Failed to export secure key '{}' in namespace '{}'", keyName, namespaceName,
                e);
            report.addFailure("SecureKey", keyName, namespaceName, e.getMessage());
          }
        }
      } catch (Exception e) {
        LOG.error("An unrecoverable error occurred during secure key export for namespace '{}'",
            namespaceName, e);
        report.addFailure("SecureKey-Batch", "ALL", namespaceName, e.getMessage());
      }
    }
    LOG.info("Finished exporting secure keys.");
  }

  public static void exportProfiles(List<NamespaceMeta> allNamespaces,
      LocationFactory locationFactory,
      ProfileService profileService,
      String backupPath, JobReport report,
      AtomicInteger profileCounter) {
    LOG.info("Starting export of compute profiles...");

    // Create a list of all namespaces to process, including the system namespace
    List<NamespaceId> namespacesToProcess = new ArrayList<>();
    namespacesToProcess.add(NamespaceId.SYSTEM);
    allNamespaces.forEach(meta -> namespacesToProcess.add(new NamespaceId(meta.getName())));

    for (NamespaceId namespaceId : namespacesToProcess) {
      String namespaceName = namespaceId.getNamespace();
      LOG.info("Starting profile export for namespace '{}'.", namespaceName);

      try {
        List<Profile> profiles = profileService.getProfiles(namespaceId, false);
        if (profiles.isEmpty()) {
          LOG.debug("No profiles found in namespace '{}'.", namespaceName);
          continue;
        }

        Location profilesDir = locationFactory.create(backupPath)
            .append("namespaces").append(namespaceName).append("profiles");
        profilesDir.mkdirs();

        for (Profile profile : profiles) {
          String profileName = profile.getName();
          try {
            profileCounter.incrementAndGet();
            Location profileFile = profilesDir.append(profileName + ".json");

            try (Writer writer = new OutputStreamWriter(profileFile.getOutputStream(),
                StandardCharsets.UTF_8)) {
              GSON.toJson(profile, writer);
            }

            report.addSuccess("Profile", profileName, namespaceName,
                profileFile.toURI().toString());
            LOG.info("Successfully exported profile '{}' in namespace '{}'", profileName,
                namespaceName);

          } catch (Exception e) {
            LOG.error("Failed to export profile '{}' in namespace '{}'", profileName, namespaceName,
                e);
            report.addFailure("Profile", profileName, namespaceName, e.getMessage());
          }
        }
      } catch (Exception e) {
        LOG.error("An unrecoverable error occurred during profile export for namespace '{}'",
            namespaceName, e);
        report.addFailure("Profile-Batch", "ALL", namespaceName,
            "Failed to list profiles: " + e.getMessage());
      }
    }
    LOG.info("Finished exporting compute profiles.");
  }
}

class SecureKeyExportDTO {

  private SecureStoreMetadata metadata;
  private String dataB64; // Decrypted data, Base64 encoded for transport

  public SecureStoreMetadata getMetadata() {
    return metadata;
  }

  public void setMetadata(SecureStoreMetadata metadata) {
    this.metadata = metadata;
  }

  public String getDataB64() {
    return dataB64;
  }

  public void setDataB64(String dataB64) {
    this.dataB64 = dataB64;
  }
}
